package saksinfo.kafka

import kotlinx.coroutines.runBlocking
import no.nav.aap.kafka.streams.extension.consume
import no.nav.aap.kafka.streams.extension.filterNotNull
import no.nav.aap.kafka.streams.extension.leftJoin
import no.nav.aap.kafka.streams.extension.produce
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Branched
import saksinfo.arena.ArenaRestClient
import saksinfo.arena.FinnesVedtakKafkaDTO
import saksinfo.arena.Response

fun topology(arenaRestClient: ArenaRestClient): Topology {
    val builder = StreamsBuilder()

    val vedtaksTable = builder.consume(Topics.vedtak)
        .produce(Tables.vedtak)

    val skalJoineMedVedtaksTopic: (key: String, value: FinnesVedtakKafkaDTO) -> Boolean =
        { _, kafkaDTO -> kafkaDTO.req.sjekkKelvin }
    builder.consume(Topics.sisteVedtak)
        .filterNotNull("filterTombstone")
        .filter { _, kafkaDTO -> kafkaDTO.res == null }
        .split()
        .branch(skalJoineMedVedtaksTopic, Branched.withConsumer { chain ->
            chain.leftJoin(Topics.sisteVedtak with Topics.vedtak, vedtaksTable)
                .map { personident, (sisteVedtakKafkaDTO, vedtakKafkaDTO) ->
                    val finnesIArena = when (sisteVedtakKafkaDTO.req.sjekkArena) {
                        true -> runBlocking { arenaRestClient.hentSisteVedtak(personident) } != null
                        false -> null
                    }
                    val svar = sisteVedtakKafkaDTO.copy(res = Response(vedtakKafkaDTO != null, finnesIArena))
                    KeyValue(personident, svar)
                }
                .filterNot { _, kafkaDTO -> kafkaDTO.res == null }
                .produce(Topics.sisteVedtak, "sisteVedtakFraKelvinOgArena")
        })
        .defaultBranch(Branched.withConsumer { chain ->
            chain.map { personident, kafkaDTO ->
                val res = if (kafkaDTO.req.sjekkArena) {
                    val respons = runBlocking { arenaRestClient.hentSisteVedtak(personident) }
                    kafkaDTO.copy(res = Response(null, respons != null))
                } else kafkaDTO
                KeyValue(personident, res)
            }
                .filterNot { _, kafkaDTO -> kafkaDTO.res == null }
                .produce(Topics.sisteVedtak, "sisteVedtakFraArena")
        })
    return builder.build()

}