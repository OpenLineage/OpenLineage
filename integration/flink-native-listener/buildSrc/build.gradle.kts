plugins {
    `kotlin-dsl`
    kotlin("plugin.serialization") version "2.0.10"
}

repositories {
    gradlePluginPortal()
    mavenCentral()
    maven("https://packages.confluent.io/maven/")
}

val downloadTaskVersion: String = "5.5.0"
val lombokPluginVersion: String = "5.0.0"
val shadowPluginVersion: String = "8.1.1"
val spotlessVersion: String = "6.25.0"
val testLoggerVersion: String = "3.2.0"

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${spotlessVersion}")
    implementation("com.adarshr.test-logger:com.adarshr.test-logger.gradle.plugin:${testLoggerVersion}")
    implementation("com.github.johnrengelman:shadow:${shadowPluginVersion}")
    implementation("de.undercouch:gradle-download-task:${downloadTaskVersion}")
    implementation("io.franzbecker.gradle-lombok:io.franzbecker.gradle-lombok.gradle.plugin:${lombokPluginVersion}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
    implementation("org.jetbrains.kotlin:kotlin-reflect")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.6.2")
}

gradlePlugin {
    plugins {
        create("commonConfig") {
            id = "io.openlineage.common-config"
            implementationClass = "io.openlineage.gradle.plugin.CommonConfigPlugin"
        }
    }
}
