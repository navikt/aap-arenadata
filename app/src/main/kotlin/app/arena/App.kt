package app.arena

import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.util.*
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.runBlocking
import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.*
import no.nav.aap.kafka.streams.extension.*
import no.nav.aap.ktor.client.AzureConfig
import no.nav.aap.ktor.config.loadConfig
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.slf4j.LoggerFactory

data class Config(
    val kafka: KStreamsConfig,
    val azure: AzureConfig,
    val arena: ArenaConfig
)

data class ArenaConfig(
    val proxyBaseUrl: String,
    val scope: String
)

private val sikkerLogg = LoggerFactory.getLogger("secureLog")

object Tables {
    val vedtak = Table("iverksatteVedtak", Topics.vedtak)
}

object Topics {
    val sisteVedtak = Topic("aap.arena-sistevedtak.v1", JsonSerde.jackson<FinnesVedtakKafkaDTO>())
    val vedtak = Topic("aap.vedtak.v1", JsonSerde.jackson<IverksettVedtakKafkaDto>())
}

fun topology(arenaRestClient: ArenaRestClient): Topology {
    val builder = StreamsBuilder()

    builder.consume(Topics.vedtak)
        .produce(Tables.vedtak)

    builder.consume(Topics.sisteVedtak)
        .filterNotNull("filterTombstone")
        .filter { _, kafkaDTO -> kafkaDTO.res == null }
        .map { personident, kafkaDTO ->
                val res  = if (kafkaDTO.req.sjekkArena){
                    val respons = runBlocking { arenaRestClient.hentSisteVedtak(personident) }
                    kafkaDTO.copy(res = Response(null, true))
                }
                else kafkaDTO
            KeyValue(personident, res)
        }
        .filterNot { _, kafkaDTO -> kafkaDTO.res == null }
        .produce(Topics.sisteVedtak, "sisteVedtakMedArenaSvar")
        return builder.build()

}

fun main() {
    embeddedServer(Netty, port = 8080, module = Application::server).start(wait = true)
}

fun Application.server(kafka: KStreams = KafkaStreams) {
    val prometheus = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    val config = loadConfig<Config>()

    install(MicrometerMetrics) { registry = prometheus }
    install(ContentNegotiation) {
        jackson {
            registerModule(JavaTimeModule())
            disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
        }
    }

    Thread.currentThread().setUncaughtExceptionHandler { _, e -> sikkerLogg.error("Uhåndtert feil", e) }
    environment.monitor.subscribe(ApplicationStopping) { kafka.close() }

    val arenaRestClient = ArenaRestClient(config.arena, config.azure)

    kafka.connect(
        config = config.kafka,
        registry = prometheus,
        topology = topology(arenaRestClient)
    )

    val statestore = kafka.getStore<IverksettVedtakKafkaDto>(Tables.vedtak.stateStoreName)

    routing {
        route("/actuator") {
            get("/metrics") {
                call.respond(prometheus.scrape())
            }
            get("/live") {
                val status = if (kafka.isLive()) HttpStatusCode.OK else HttpStatusCode.InternalServerError
                call.respond(status, "vedtak")
            }
            get("/ready") {
                val status = if (kafka.isReady()) HttpStatusCode.OK else HttpStatusCode.InternalServerError
                call.respond(status, "vedtak")
            }
        }
        route("/kelvin") {
            get("/vedtak") {
                val personident = call.parameters.getOrFail("personident")
                call.respond(statestore[personident])
            }
        }
        route("/arena") {
            get("/vedtak") {
                val personident = call.parameters.getOrFail("personident")
                val arenarespons = arenaRestClient.hentSisteVedtak(personident)
                call.respond(arenarespons)
            }
        }
    }
}