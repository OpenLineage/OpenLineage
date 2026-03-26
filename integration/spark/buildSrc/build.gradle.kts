// Upgraded to Gradle 8.13 with Shadow plugin 9.4.0 for Java 21 bytecode support
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
val shadowPluginVersion: String = "9.4.0"
val spotlessVersion: String = "6.13.0"
val kotlinVersion: String = "2.0.21"

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${spotlessVersion}")
    implementation("com.gradleup.shadow:shadow-gradle-plugin:${shadowPluginVersion}")
    implementation("de.undercouch:gradle-download-task:${downloadTaskVersion}")
    implementation("io.freefair.gradle:lombok-plugin:${lombokPluginVersion}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:${kotlinVersion}")
    implementation("org.jetbrains.kotlin:kotlin-reflect:${kotlinVersion}")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")
    implementation("org.javassist:javassist:3.30.2-GA")
    implementation("com.adarshr:gradle-test-logger-plugin:4.0.0")
    
    // Force Kotlin 2.0.21 to match Gradle 8.13's embedded Kotlin
    constraints {
        implementation("org.jetbrains.kotlin:kotlin-stdlib:${kotlinVersion}")
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk7:${kotlinVersion}")
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8:${kotlinVersion}")
        implementation("org.jetbrains.kotlin:kotlin-reflect:${kotlinVersion}")
        implementation("org.jetbrains.kotlin:kotlin-metadata-jvm:${kotlinVersion}")
    }
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
