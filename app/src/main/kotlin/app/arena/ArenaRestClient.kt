package app.arena

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.plugins.logging.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.jackson.*
import no.nav.aap.ktor.client.AzureAdTokenProvider
import no.nav.aap.ktor.client.AzureConfig
import org.slf4j.LoggerFactory

private val sikkerLogg = LoggerFactory.getLogger("secureLog")

class ArenaRestClient(
    private val arenaConfig: ArenaConfig,
    azureConfig: AzureConfig
) {
    private val tokenProvider = AzureAdTokenProvider(azureConfig, arenaConfig.scope)

    suspend fun hentSisteVedtak(
        fnr: String
    ): ArenaResponse? {
        val token = tokenProvider.getClientCredentialToken()
        return try {
            httpClient.get("${arenaConfig.proxyBaseUrl}/arena/vedtak/$fnr") {
                accept(ContentType.Application.Json)
                // header("fnr", fnr)
                bearerAuth(token)
                contentType(ContentType.Application.Json)
            }
                .body()
        } catch (exception: Exception){
            null
        }
    }

    private val httpClient = HttpClient(CIO) {
        install(HttpTimeout)
        install(HttpRequestRetry)
        install(Logging) {
            level = LogLevel.BODY
            logger = object : Logger {
                override fun log(message: String) {
                    sikkerLogg.debug("respons fra Arena: $message")
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