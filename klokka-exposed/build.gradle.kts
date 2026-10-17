import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm)
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
    // api: the public overloads take JdbcTransaction. Applications already depend on Exposed,
    // and their own version wins conflict resolution.
    api(libs.exposed.jdbc)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(project(":klokka-postgres"))
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.postgresql)
    testRuntimeOnly(libs.slf4j.simple)
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
