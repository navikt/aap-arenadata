package saksinfo

import no.nav.aap.kafka.streams.KStreamsConfig
import no.nav.aap.ktor.client.AzureConfig
import java.net.URL

data class Config(
    val kafka: KStreamsConfig,
    val azure: AzureConfig,
    val arena: ArenaConfig,
    val oauth: OauthConfig
)

data class OauthConfig(
    val azure: IssuerConfig
)

data class IssuerConfig(
    val issuer: String,
    val audience: String,
    val jwksUrl: URL
)

data class ArenaConfig(
    val proxyBaseUrl: String,
    val scope: String
)