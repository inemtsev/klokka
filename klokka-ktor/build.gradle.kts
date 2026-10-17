import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    `java-library`
}

kotlin {
    jvmToolchain(17)

    compilerOptions {
        jvmTarget.set(JvmTarget.JVM_17)
    }

    explicitApi()

    sourceSets.all {
        languageSettings {
            optIn("kotlin.time.ExperimentalTime")
        }
    }
}

dependencies {
    api(project(":klokka-core"))
    api(libs.ktor.server.core)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.ktor.server.test.host)
    testImplementation(libs.kotlinx.coroutines.test)
    // The transactional-enqueue samples run against a real Postgres, as an application would.
    testImplementation(project(":klokka-postgres"))
    testImplementation(project(":klokka-exposed"))
    testImplementation(libs.postgresql)
    testImplementation(libs.testcontainers.postgresql)
}

tasks.withType<Test> {
    useJUnitPlatform()
    // Same Docker API pin as klokka-postgres: Testcontainers 1.21.x defaults to an API
    // version Docker Engine 29+ removed. A caller-provided value always wins.
    if (System.getenv("DOCKER_API_VERSION") == null) {
        environment("DOCKER_API_VERSION", "1.44")
        systemProperty("api.version", "1.44")
    }
}
