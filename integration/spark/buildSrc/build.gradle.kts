plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

val spotlessVersion: String = "6.13.0"

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${spotlessVersion}")
}

gradlePlugin {
    plugins {
        create("printSourceSetConfiguration") {
            id = "io.openlineage.print-source-set-configuration"
            implementationClass = "io.openlineage.gradle.plugin.PrintSourceSetConfigurationPlugin"
        }

        create("standardSpotless") {
            id = "io.openlineage.standard-spotless"
            implementationClass = "io.openlineage.gradle.plugin.StandardSpotlessPlugin"
        }

        create("scala213Variant") {
            id = "io.openlineage.scala213-variant"
            implementationClass = "io.openlineage.gradle.plugin.Scala213VariantPlugin"
        }

        create("commonJavaConfig") {
            id = "io.openlineage.common-java-config"
            implementationClass = "io.openlineage.gradle.plugin.CommonJavaConfigPlugin"
        }
    }
}
