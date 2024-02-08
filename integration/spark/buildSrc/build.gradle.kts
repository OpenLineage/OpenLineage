plugins {
    `kotlin-dsl`
}

repositories {
    gradlePluginPortal()
    mavenCentral()
}

val lombokPluginVersion: String = "8.4"
val shadowPluginVersion: String = "8.1.1"
val spotlessVersion: String = "6.13.0"

dependencies {
    implementation("com.diffplug.spotless:spotless-plugin-gradle:${spotlessVersion}")
    implementation("com.github.johnrengelman:shadow:${shadowPluginVersion}")
    implementation("io.freefair.gradle:lombok-plugin:${lombokPluginVersion}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
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
    }
}
