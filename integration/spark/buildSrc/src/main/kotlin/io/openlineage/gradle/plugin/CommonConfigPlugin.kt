/**
 * Copyright 2018-2026 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin

import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.diffplug.spotless.FormatterFunc
import com.adarshr.gradle.testlogger.TestLoggerPlugin
import com.adarshr.gradle.testlogger.TestLoggerExtension
import io.freefair.gradle.plugins.lombok.LombokExtension
import io.freefair.gradle.plugins.lombok.LombokPlugin
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.plugins.quality.Pmd
import org.gradle.api.plugins.quality.PmdExtension
import org.gradle.api.plugins.quality.PmdPlugin
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.compile.ForkOptions
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.scala.ScalaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.*
import java.io.File

/**
 * A Gradle plugin that consolidates common configurations for Java projects.
 * This plugin integrates functionalities like PMD, Lombok, Spotless, and source set configuration printing.
 * It simplifies the process of setting up these components for a project.
 *
 * To use this plugin, include it in your build script and configure the properties as needed:
 *
 * ```kotlin
 * plugins {
 *     id("io.openlineage.common-config")
 * }
 * ```
 *
 * @see org.gradle.api.Plugin
 */
class CommonConfigPlugin : Plugin<Project> {
    override fun apply(target: Project) {
        configureExtension(target)
        configureJava(target)
        configurePmd(target)
        configureLombok(target)
        configureSpotless(target)
        configurePrintSourceSetTask(target)
        configureTestLogger(target)
    }

    private fun getPluginExtension(target: Project): CommonConfigPluginExtension =
        target.extensions.getByType<CommonConfigPluginExtension>()

    private fun configureExtension(target: Project) {
        val extension = target.extensions.create<CommonConfigPluginExtension>("commonConfig")
        extension.lombokVersion.convention("1.18.30")
    }

    private fun configureJava(target: Project) = target.plugins.withType<JavaPlugin> {
        target.extensions.getByType<JavaPluginExtension>().apply {
            sourceCompatibility = JavaVersion.VERSION_1_8
            targetCompatibility = JavaVersion.VERSION_1_8
        }

        target.repositories.mavenCentral()
        target.repositories.mavenLocal()

        if (target.hasProperty("java.compile.home")) {
            // This is necessary as we want to compile classes on CI always with Java 17 while
            // testing it on different Java versions to allow running different Spark versions.
            target.tasks.withType<JavaCompile>().configureEach  {
                // enable compilation in a separate daemon process
                options.setFork(true)
                options.forkOptions.javaHome = File(target.findProperty("java.compile.home").toString())
            }
        }

        target.tasks.withType<JavaCompile>().configureEach  {
            doFirst {
                if (System.getenv().containsKey("CI") && !target.hasProperty("java.compile.home")) {
                    // never run compile on CI without property being set
                    throw RuntimeException("java.compile.home should be always set on CI env")
                }
            }
        }

        target.tasks.withType<Test> {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
                showStandardStreams = true
            }

            if (target.hasProperty("java.test.home")) {
                // Allow running tests with a specific JDK (e.g. Java 17) independently of the
                // JDK used to run Gradle. Mirrors the java.compile.home mechanism for compilation.
                executable = "${target.findProperty("java.test.home")}/bin/java"
            }

            if (JavaVersion.current() >= JavaVersion.VERSION_17) {
                jvmArgs(
                    "--add-opens=java.base/java.lang=ALL-UNNAMED",
                    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
                    "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
                    "--add-opens=java.base/java.io=ALL-UNNAMED",
                    "--add-opens=java.base/java.net=ALL-UNNAMED",
                    "--add-opens=java.base/java.nio=ALL-UNNAMED",
                    "--add-opens=java.base/java.util=ALL-UNNAMED",
                    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
                    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
                    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
                    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
                    "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
                    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
                    "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                )
            }
        }
    }

    private fun configurePmd(target: Project) = target.plugins.withType<PmdPlugin> {
        with(target.extensions.getByType<PmdExtension>()) {
            isConsoleOutput = true
            toolVersion = "6.46.0"
            rulesMinimumPriority.set(5)
            ruleSetFiles = target.rootProject.files("pmd-openlineage.xml")
            ruleSets = listOf()
            isIgnoreFailures = false
        }

        target.tasks.named<Pmd>("pmdMain") {
            this.reports.html.required.set(true)
        }
        target.tasks.named<Pmd>("pmdTest") {
            this.reports.html.required.set(true)
            this.ruleSetFiles = target.rootProject.files("pmd-openlineage-test.xml")
        }
    }

    private fun configureLombok(target: Project) = target.plugins.withType<LombokPlugin> {
        val commonConfigExtension = getPluginExtension(target)
        with(target.extensions.getByType<LombokExtension>()) {
            version.set(commonConfigExtension.lombokVersion)
        }
    }

    private fun configureTestLogger(target: Project) = target.plugins.withType<TestLoggerPlugin> {
        target.extensions.configure<TestLoggerExtension> {
            showExceptions = false
            showStackTraces = false
            showStandardStreams = true
        }
    }

    private fun configureSpotless(target: Project) = target.plugins.withType<SpotlessPlugin> {
        target.extensions.configure<SpotlessExtension> {
            java {
                googleJavaFormat()
                removeUnusedImports()
                custom("disallowWildcardImports", WildcardImportChecker)
            }

            // disable spotless tasks for Java 8
            if (JavaVersion.current() == JavaVersion.VERSION_1_8) {
                isEnforceCheck = false
            }
        }
    }

    private fun configurePrintSourceSetTask(target: Project) {
        target.tasks.register("printSourceSetConfiguration") {
            doLast {
                val sourceSets = target.extensions.getByType(SourceSetContainer::class.java)
                sourceSets.forEach { srcSet ->
                    println("[${srcSet.name}]")
                    println("-->Source directories: ${srcSet.allJava.srcDirs}")
                    println("-->Output directories: ${srcSet.output.classesDirs.files}")
                    println("-->Compile classpath:")
                    srcSet.compileClasspath.files.sortedBy { it.path }.forEach {
                        println("  ${it.path}")
                    }
                    println("")
                }
            }
        }
    }
}

// Top-level singleton so it's serializable for Gradle 9 task input fingerprinting.
// Kotlin lambdas are not serializable; object declarations are.
private object WildcardImportChecker : FormatterFunc, java.io.Serializable {
    private const val serialVersionUID = 1L
    // Matches only wildcard imports: import foo.bar.*;
    // Note: \\.\\* matches a literal ".*" — the original lambda used \\.*
    // which in regex means "zero or more literal dots", incorrectly matching
    // all imports. The correct pattern needs explicit \\. then \\*.
    override fun apply(input: String): String {
        val regex = Regex("^import\\s+[\\w.]+\\.\\*;$", RegexOption.MULTILINE)
        val m = regex.find(input)
        if (m != null) {
            throw RuntimeException("Wildcard imports are disallowed - ${m.groupValues}")
        }
        return input
    }
}
