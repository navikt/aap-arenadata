package saksinfo

import com.auth0.jwk.JwkProviderBuilder
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
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
import io.micrometer.prometheus.PrometheusConfig
import io.micrometer.prometheus.PrometheusMeterRegistry
import no.nav.aap.kafka.streams.v2.Streams
import no.nav.aap.kafka.streams.v2.KafkaStreams
import no.nav.aap.ktor.config.loadConfig
import org.slf4j.LoggerFactory
import saksinfo.arena.ArenaoppslagRestClient
import saksinfo.kafka.Tables
import saksinfo.kafka.topology
import java.time.LocalDate
import java.util.concurrent.TimeUnit

private val sikkerLogg = LoggerFactory.getLogger("secureLog")

fun main() {
    embeddedServer(Netty, port = 8080, module = Application::server).start(wait = true)
}

private fun Application.server(kafka: Streams = KafkaStreams()) {
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

    val arenaoppslagRestClient= ArenaoppslagRestClient(config.arenaoppslag, config.azure)

    kafka.connect(
        config = config.kafka,
        registry = prometheus,
        topology = topology(arenaoppslagRestClient)
    )

    val statestore = kafka.getStore(Tables.vedtak)

    routing {
        actuators(prometheus, kafka)
        route("/v1/vedtak") {
            authenticate{
                get {
                    val personident = call.request.header("X-personident") ?: throw BadRequestException("Header 'X-personident' må være satt")
                    val arenarespons = arenaoppslagRestClient.hentVedtak(personident)
                    val kelvinrespons = statestore[personident]
                    if (arenarespons.isNotEmpty()) {
                        call.respond(arenarespons.map {vedtak ->
                                Vedtaksdata(
                                    harVedtak = true,
                                    fom = vedtak.fraDato.toString(),
                                    tom = vedtak.tilDato.toString(),
                                    kilde = "arena"
                                )
                            }.toList()
                        )
                    } else if (kelvinrespons != null) {
                        call.respond(Vedtaksdata(harVedtak = true, kilde = "kelvin"))
                    } else call.respond(Vedtaksdata(harVedtak = false, kilde = ""))
                }
            }
        }
    }
}