import org.gradle.kotlin.dsl.testImplementation

plugins {
    kotlin("jvm") version "2.3.10"
    id("org.jlleitschuh.gradle.ktlint") version "14.2.0"
}

group = "kkafka"
version = "0.0.1"

val reactorVersion: String by project

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.kafka:kafka-clients:4.2.0")
    implementation("io.projectreactor:reactor-core:$reactorVersion")
    implementation("org.slf4j:slf4j-api:1.7.36")
    implementation("ch.qos.logback:logback-classic:1.5.32")
    implementation("ch.qos.logback:logback-core:1.5.32")

    implementation("io.projectreactor.kotlin:reactor-kotlin-extensions:1.3.0")

    testImplementation(kotlin("test"))

    testImplementation("io.projectreactor:reactor-test:$reactorVersion")

    testImplementation(platform("org.testcontainers:testcontainers-bom:2.0.3"))
    testImplementation("org.testcontainers:testcontainers-kafka")
    testImplementation("org.testcontainers:testcontainers-junit-jupiter")
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}

ktlint {
    version.set("1.6.0")
}
