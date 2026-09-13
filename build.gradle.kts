import com.vanniktech.maven.publish.JavadocJar
import com.vanniktech.maven.publish.KotlinJvm
import com.vanniktech.maven.publish.KotlinMultiplatform
import com.vanniktech.maven.publish.MavenPublishBaseExtension
import io.gitlab.arturbosch.detekt.extensions.DetektExtension
import org.jlleitschuh.gradle.ktlint.KtlintExtension

plugins {
    alias(libs.plugins.kotlin.multiplatform) apply false
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.ktlint) apply false
    alias(libs.plugins.detekt) apply false
    alias(libs.plugins.binary.compatibility.validator)
    alias(libs.plugins.maven.publish) apply false
    alias(libs.plugins.dokka)
}

// The root project aggregates every published module into one HTML site (`./gradlew dokkaGenerate`,
// output in build/dokka/html), which CI deploys to GitHub Pages. Per-module Dokka output also
// becomes the -javadoc.jar; before this, Central received an empty placeholder jar.
dependencies {
    subprojects.forEach { dokka(it) }
}

// No modules are excluded from API validation yet. klokka-core carries a checked-in
// baseline at klokka-core/api/klokka-core.api once apiDump has run.
apiValidation {
}

allprojects {
    group = "com.eventslooped"
    version = "0.1.0-SNAPSHOT"
}

// One-line, consumer-facing POM description per published module.
val moduleDescriptions = mapOf(
    "klokka-core" to "Framework-free job runtime: typed handlers, retries, recurring schedules, and the JobStore SPI",
    "klokka-ktor" to "Ktor plugin: install(Klokka), lifecycle wiring, and the configuration DSL",
    "klokka-postgres" to "Postgres store: SKIP LOCKED claims, LISTEN/NOTIFY push wake-ups, bundled migrations",
    "klokka-dashboard" to "Server-rendered dashboard: states overview, job detail, requeue; mounts behind your own auth",
)

subprojects {
    apply(plugin = "org.jlleitschuh.gradle.ktlint")
    apply(plugin = "io.gitlab.arturbosch.detekt")

    configure<KtlintExtension> {
        // Lenient for now: report style issues without failing the build. Tighten once
        // conventions (and an .editorconfig) have settled.
        ignoreFailures.set(true)
    }

    configure<DetektExtension> {
        buildUponDefaultConfig = true
        ignoreFailures = true
    }

    apply(plugin = "com.vanniktech.maven.publish")
    apply(plugin = "org.jetbrains.dokka")

    // Deferred: the module's own build script applies kotlin-jvm or kotlin-multiplatform after
    // this block runs, and configure(KotlinJvm(...))/configure(KotlinMultiplatform(...)) requires
    // it. Puts real KDoc in the -javadoc.jar, rather than the empty placeholder the publish plugin
    // emits with no documentation engine configured.
    plugins.withId("org.jetbrains.kotlin.jvm") {
        configure<MavenPublishBaseExtension> {
            configure(KotlinJvm(javadocJar = JavadocJar.Dokka("dokkaGeneratePublicationHtml")))
        }
    }
    plugins.withId("org.jetbrains.kotlin.multiplatform") {
        configure<MavenPublishBaseExtension> {
            configure(KotlinMultiplatform(javadocJar = JavadocJar.Dokka("dokkaGeneratePublicationHtml")))
        }
    }

    configure<MavenPublishBaseExtension> {
        publishToMavenCentral()
        // Central rejects unsigned uploads, but gating on the key keeps keyless local/CI
        // publishes (publishToMavenLocal) working without a GPG setup.
        if (providers.gradleProperty("signingInMemoryKey").isPresent) {
            signAllPublications()
        }
        coordinates("com.eventslooped", name, version.toString())
        pom {
            name.set(this@subprojects.name)
            description.set(moduleDescriptions.getValue(this@subprojects.name))
            url.set("https://github.com/inemtsev/klokka")
            licenses {
                license {
                    name.set("The Apache License, Version 2.0")
                    url.set("https://www.apache.org/licenses/LICENSE-2.0.txt")
                }
            }
            developers {
                developer {
                    id.set("inemtsev")
                    name.set("Ilya Nemtsev")
                }
            }
            scm {
                url.set("https://github.com/inemtsev/klokka")
                connection.set("scm:git:https://github.com/inemtsev/klokka.git")
                developerConnection.set("scm:git:ssh://git@github.com/inemtsev/klokka.git")
            }
        }
    }
}
