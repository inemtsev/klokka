import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
}

group = "com.eventslooped"
version = "0.1.0-SNAPSHOT"

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
    // The listener half of LISTEN/NOTIFY has no standard JDBC API; PGConnection is pgjdbc.
    // Nobody reaches Postgres over JDBC without this driver anyway, and an application
    // pinning its own version wins conflict resolution.
    implementation(libs.postgresql)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers.postgresql)
    // Surfaces Testcontainers/driver logs in test output; no main-source logging dependency.
    testRuntimeOnly(libs.slf4j.simple)
}

tasks.withType<Test> {
    useJUnitPlatform()
    // Testcontainers 1.21.x pins Docker API 1.32, which Docker Engine 29+ removed
    // (it answers /v1.32/* with a sanitized 400). 1.44 = Engine 25, old enough for any
    // 2024+ daemon. A caller-provided value always wins.
    if (System.getenv("DOCKER_API_VERSION") == null) {
        environment("DOCKER_API_VERSION", "1.44")
        systemProperty("api.version", "1.44")
    }
}
