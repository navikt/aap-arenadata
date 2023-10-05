package saksinfo.arena

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import io.ktor.client.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.logging.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import io.prometheus.client.Summary
import kotlinx.coroutines.runBlocking
import no.nav.aap.ktor.client.AzureAdTokenProvider
import no.nav.aap.ktor.client.AzureConfig
import org.slf4j.LoggerFactory
import saksinfo.ArenaoppslagConfig
import java.util.*


private const val ARENAOPPSLAG_CLIENT_SECONDS_METRICNAME = "arenaoppslag_client_seconds"
private val sikkerLogg = LoggerFactory.getLogger("secureLog")
private val clientLatencyStats: Summary = Summary.build()
    .name(ARENAOPPSLAG_CLIENT_SECONDS_METRICNAME)
    .quantile(0.5, 0.05) // Add 50th percentile (= median) with 5% tolerated error
    .quantile(0.9, 0.01) // Add 90th percentile with 1% tolerated error
    .quantile(0.99, 0.001) // Add 99th percentile with 0.1% tolerated error
    .help("Latency inntektskomponenten, in seconds")
    .register()
private val objectMapper = jacksonObjectMapper()
    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    .registerModule(JavaTimeModule())

class ArenaoppslagRestClient(
    private val arenaoppslagConfig: ArenaoppslagConfig,
    azureConfig: AzureConfig
) {
    private val tokenProvider = AzureAdTokenProvider(azureConfig, arenaoppslagConfig.scope)

    fun hentVedtak(fnr: String):List<Vedtak> =
        clientLatencyStats.startTimer().use {
            runBlocking {
                httpClient.post("${arenaoppslagConfig.proxyBaseUrl}/vedtak"){
                    accept(ContentType.Application.Json)
                    header("Nav-Call-Id", UUID.randomUUID())
                    bearerAuth(tokenProvider.getClientCredentialToken())
                    contentType(ContentType.Application.Json)
                    HeaderValueParam("fnr", fnr)
                }
                    .bodyAsText()
                    .also { svar -> sikkerLogg.info("Svar fra inntektskomponenten:\n$svar") }
                    .let(objectMapper::readValue)

            }
        }


    private val httpClient = HttpClient(CIO) {
        install(HttpTimeout)
        install(HttpRequestRetry)
        install(Logging) {
            level = LogLevel.BODY
            logger = object : Logger {
                private var logBody = false
                override fun log(message: String) {
                    when {
                        message == "BODY START" -> logBody = true
                        message == "BODY END" -> logBody = false
                        logBody -> sikkerLogg.debug("respons fra Inntektskomponenten: $message")
                    }
                }
            }
        }

        install(ContentNegotiation) {
            jackson {
                disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                registerModule(JavaTimeModule())
            }
        }
    }
}