package saksinfo.kafka


import no.nav.aap.kafka.streams.v2.Topic
import no.nav.aap.kafka.streams.v2.serde.JsonSerde
import saksinfo.arena.FinnesVedtakKafkaDTO

object Topics {
    val sisteVedtak = Topic("aap.arena-sistevedtak.v1", JsonSerde.jackson<FinnesVedtakKafkaDTO>())
    val vedtak = Topic("aap.vedtak.v1", JsonSerde.jackson<IverksettVedtakKafkaDto>())
}