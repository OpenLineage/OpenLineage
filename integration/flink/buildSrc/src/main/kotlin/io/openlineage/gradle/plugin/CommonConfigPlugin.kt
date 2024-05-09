/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 **/

package io.openlineage.gradle.plugin

import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.gradle.spotless.SpotlessPlugin
import com.diffplug.spotless.FormatterFunc
import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.api.plugins.quality.Pmd
import org.gradle.api.plugins.quality.PmdExtension
import org.gradle.api.plugins.quality.PmdPlugin
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.*

class CommonConfigPlugin : Plugin<Project> {
    override fun apply(target: Project) {
        configurePmd(target)
        configureJava(target)
        configureSpotless(target)
        configurePrintSourceSetTask(target)
    }

    private fun configureJava(target: Project) = target.plugins.withType<JavaPlugin> {
        target.extensions.getByType<JavaPluginExtension>().apply {
            sourceCompatibility = JavaVersion.VERSION_11
            targetCompatibility = JavaVersion.VERSION_11
        }

        target.repositories.mavenCentral()
        target.repositories.maven("https://packages.confluent.io/maven/")
        target.repositories.mavenLocal()

        target.tasks.withType<Test> {
            maxParallelForks = 3
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
                showStandardStreams = true
            }
            systemProperties = mapOf(
                "junit.platform.output.capture.stdout" to "true",
                "junit.platform.output.capture.stderr" to "true",
            )
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

    private fun configureSpotless(target: Project) = target.plugins.withType<SpotlessPlugin> {
        val disallowWildcardImports = FormatterFunc { text ->
            val regex = Regex("^import .*\\.\\*;$")
            val m = regex.find(text)
            if (m != null) {
                throw RuntimeException("Wildcard imports are disallowed - ${m.groupValues}")
            }
            text
        }

        target.extensions.configure<SpotlessExtension> {
            java {
                targetExclude("**/proto/**")
                googleJavaFormat()
                removeUnusedImports()
                custom("disallowWildcardImports", disallowWildcardImports)
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