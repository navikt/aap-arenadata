package saksinfo

import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.ktor.server.application.*
import io.ktor.server.auth.*
import io.ktor.server.auth.jwt.*
import io.ktor.server.engine.*
import io.ktor.server.metrics.micrometer.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import io.ktor.server.util.*
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import kotlinx.coroutines.runBlocking
import no.nav.aap.kafka.serde.json.JsonSerde
import no.nav.aap.kafka.streams.*
import no.nav.aap.kafka.streams.extension.*
import no.nav.aap.ktor.config.loadConfig
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Branched
import org.slf4j.LoggerFactory
import saksinfo.arena.ArenaRestClient
import saksinfo.arena.FinnesVedtakKafkaDTO
import saksinfo.arena.Response
import saksinfo.kafka.IverksettVedtakKafkaDto
import saksinfo.kafka.Tables
import saksinfo.kafka.Topics
import saksinfo.kafka.topology
import java.util.concurrent.TimeUnit

private val sikkerLogg = LoggerFactory.getLogger("secureLog")

fun main() {
    embeddedServer(Netty, port = 8080, module = Application::server).start(wait = true)
}

private fun Application.server(kafka: KStreams = KafkaStreams) {
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

    val jwkProvider = JwkProviderBuilder(config.oauth.azure.jwksUrl)
        .cached(10, 24, TimeUnit.HOURS)
        .rateLimited(10, 1, TimeUnit.MINUTES)
        .build()
    authentication {
        jwt {
            realm = "hent saksinfo"
            verifier(jwkProvider, config.oauth.azure.issuer)
            validate { credential ->
                if (credential.payload.audience.contains(config.oauth.azure.audience)) JWTPrincipal(credential.payload) else null
            }
        }
    }

    val arenaRestClient = ArenaRestClient(config.arena, config.azure)

    kafka.connect(
        config = config.kafka,
        registry = prometheus,
        topology = topology(arenaRestClient)
    )

    val statestore = kafka.getStore<IverksettVedtakKafkaDto>(Tables.vedtak.stateStoreName)

    routing {
        actuators(prometheus, kafka)
        route("/v1/vedtak") {
            authenticate{
                get {
                    val personident = call.request.header("X-personident") ?: throw BadRequestException("Header 'X-personident' må være satt")
                    val arenarespons = arenaRestClient.hentSisteVedtak(personident)
                    val kelvinrespons = statestore[personident]
                    if (arenarespons != null) {
                        call.respond(Vedtaksdata(true, "arena"))
                    } else if (kelvinrespons != null) {
                        call.respond(Vedtaksdata(true, "kelvin"))
                    } else call.respond(Vedtaksdata(false, ""))
                }
            }
        }
    }
}