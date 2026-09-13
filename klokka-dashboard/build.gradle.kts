import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
    // Tests declare @Serializable sample payloads.
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
    // The fail-closed mount inspects the authenticate { } route selector.
    implementation(libs.ktor.server.auth)
    implementation(libs.ktor.server.html.builder)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.ktor.server.test.host)
    testImplementation(project(":klokka-ktor"))
}

tasks.withType<Test> {
    useJUnitPlatform()
}
