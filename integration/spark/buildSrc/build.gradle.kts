plugins {
    `kotlin-dsl`
    kotlin("plugin.serialization") version "2.0.21"
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

val downloadTaskVersion: String = "5.6.0"
val lombokPluginVersion: String = "8.6"
val shadowPluginVersion: String = "8.1.1"
val spotlessVersion: String = "6.13.0"

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${spotlessVersion}")
    implementation("com.github.johnrengelman:shadow:${shadowPluginVersion}")
    implementation("de.undercouch:gradle-download-task:${downloadTaskVersion}")
    implementation("io.freefair.gradle:lombok-plugin:${lombokPluginVersion}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
    implementation("org.javassist:javassist:3.30.2-GA")
    implementation("com.adarshr:gradle-test-logger-plugin:4.0.0")
}

gradlePlugin {
    plugins {
        create("commonConfig") {
            id = "io.openlineage.common-config"
            implementationClass = "io.openlineage.gradle.plugin.CommonConfigPlugin"
        }

        create("scalaVariants") {
            id = "io.openlineage.scala-variants"
            implementationClass = "io.openlineage.gradle.plugin.ScalaVariantsPlugin"
        }

        create("jarVerification") {
            id = "io.openlineage.jar-verification"
            implementationClass = "io.openlineage.gradle.plugin.JarVerificationPlugin"
        }
    }
}
