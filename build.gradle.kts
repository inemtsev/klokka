import io.gitlab.arturbosch.detekt.extensions.DetektExtension
import org.jlleitschuh.gradle.ktlint.KtlintExtension

plugins {
    alias(libs.plugins.kotlin.multiplatform) apply false
    alias(libs.plugins.kotlin.serialization) apply false
    alias(libs.plugins.ktlint) apply false
    alias(libs.plugins.detekt) apply false
    alias(libs.plugins.binary.compatibility.validator)
}

// No modules are excluded from API validation yet. klokka-core carries a checked-in
// baseline at klokka-core/api/klokka-core.api once apiDump has run.
apiValidation {
}

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
}
