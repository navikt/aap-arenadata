package saksinfo.kafka

import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.Topic
import saksinfo.arena.FinnesVedtakKafkaDTO

object Topics {
    val sisteVedtak = Topic("aap.arena-sistevedtak.v1", JsonSerde.jackson<FinnesVedtakKafkaDTO>())
    val vedtak = Topic("aap.vedtak.v1", JsonSerde.jackson<IverksettVedtakKafkaDto>())
}