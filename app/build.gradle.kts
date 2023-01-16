val aapLibVersion = "3.5.36"
val ktorVersion = "2.2.2"

plugins {
    id("io.ktor.plugin")
    id("com.github.johnrengelman.shadow")
    application
}

application {
    mainClass.set("app.saksinfo.AppKt")
}

dependencies {
    implementation("com.github.navikt.aap-libs:ktor-utils:$aapLibVersion")
    implementation("com.github.navikt.aap-libs:ktor-auth-azuread:$aapLibVersion")
    implementation("com.github.navikt.aap-libs:kafka:$aapLibVersion")

    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation:$ktorVersion")
    implementation("io.ktor:ktor-server-auth:$ktorVersion")
    implementation("io.ktor:ktor-server-auth-jwt:$ktorVersion")
    implementation("io.ktor:ktor-server-metrics-micrometer:$ktorVersion")

    implementation("io.ktor:ktor-client-jackson:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-logging-jvm:$ktorVersion")
    implementation("io.ktor:ktor-client-auth:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation:$ktorVersion")

    implementation("io.ktor:ktor-serialization-jackson:$ktorVersion")

    implementation("io.micrometer:micrometer-registry-prometheus:1.10.2")
    implementation("ch.qos.logback:logback-classic:1.4.5")
    implementation("io.ktor:ktor-server-auth-jvm:2.2.2")
    implementation("io.ktor:ktor-server-core-jvm:2.2.2")
    implementation("io.ktor:ktor-server-auth-jwt-jvm:2.2.2")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:7.2")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:2.14.1")

    testImplementation(kotlin("test"))
    testImplementation("io.ktor:ktor-server-test-host:$ktorVersion")
    testImplementation("com.github.navikt.aap-libs:kafka-test:$aapLibVersion")
}