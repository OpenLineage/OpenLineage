/*
* SPDX-License-Identifier: Apache-2.0
* Copyright 2018-2023 contributors to the OpenLineage project
*/

package io.openlineage.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.tasks.Jar
import org.gradle.kotlin.dsl.*

/**
 * A Gradle plugin that configures a project for Scala 2.13 development.
 * It manages the setup of source sets, tasks, and configurations specific to Scala 2.13,
 * facilitating the development and testing of Scala-based applications.
 *
 * To apply this plugin, add the following to your build script:
 *
 * ```kotlin
 * plugins {
 *     id("io.openlineage.scala213-variant")
 * }
 * ```
 *
 * @see org.gradle.api.Plugin
 */
class Scala213VariantPlugin : Plugin<Project> {
    override fun apply(target: Project) {
        configurePluginExtension(target)
        configurePlugins(target)
        configureSourceSets(target)
        configureConfigurations(target)
        configureTasks(target)
        configureArtifacts(target)
    }

    private fun getSourceSetContainer(target: Project) = target.extensions.getByType<SourceSetContainer>()

    private fun getPluginExtension(target: Project) = target.extensions.getByType<Scala213VariantPluginExtension>()

    private fun configurePlugins(target: Project) {
        target.plugins.apply("java-library")
    }

    private fun configurePluginExtension(target: Project) {
        val extension = target.extensions.create<Scala213VariantPluginExtension>("scala213")
        val moduleName = target.name
        extension.archiveBaseName.convention("openlineage-spark-${moduleName}")
    }

    private fun configureSourceSets(target: Project) {
        val sourceSets = getSourceSetContainer(target)

        val scala213SourceSet = sourceSets.create("scala213") {
            java {
                srcDir("src/main/java")
            }
            resources {
                srcDir("src/main/resources")
            }
        }

        sourceSets.create("testScala213") {
            compileClasspath += scala213SourceSet.output
            runtimeClasspath += scala213SourceSet.output

            java {
                srcDir("src/test/java")
            }
            resources {
                srcDir("src/test/resources")
            }
        }
    }

    private fun configureConfigurations(target: Project) {
        val configurations = target.configurations

        val scala213Api = configurations.create("scala213Api")

        configurations.create("scala213ApiElements") {
            extendsFrom(scala213Api)
            isCanBeResolved = false
            isCanBeConsumed = true
        }

        val scala213Implementation = configurations.named("scala213Implementation") {
            extendsFrom(scala213Api)
        }

        val scala213RuntimeOnly = configurations.named("scala213RuntimeOnly")

        val scala213CompileOnly = configurations.named("scala213CompileOnly")

        configurations.create("scala213RuntimeElements") {
            extendsFrom(scala213Implementation.get(), scala213RuntimeOnly.get())
            isCanBeResolved = false
            isCanBeConsumed = true
        }

        configurations.named("scala213CompileClasspath") {
            extendsFrom(scala213CompileOnly.get(), scala213Implementation.get())
            isCanBeResolved = true
        }

        configurations.named("scala213RuntimeClasspath") {
            extendsFrom(scala213Implementation.get(), scala213RuntimeOnly.get())
            isCanBeResolved = true
        }

        val testScala213Implementation = configurations.named("testScala213Implementation") {
            extendsFrom(scala213Implementation.get())
        }

        val testScala213RuntimeOnly = configurations.named("testScala213RuntimeOnly") {
            extendsFrom(scala213RuntimeOnly.get())
        }

        val testScala213CompileOnly = configurations.named("testScala213CompileOnly")

        configurations.named("testScala213CompileClasspath") {
            extendsFrom(testScala213CompileOnly.get(), testScala213Implementation.get())
            isCanBeResolved = true
        }

        configurations.named("testScala213RuntimeClasspath") {
            extendsFrom(testScala213Implementation.get(), testScala213RuntimeOnly.get())
            isCanBeResolved = true
        }
    }

    private fun configureTasks(target: Project) {
        val tasks = target.tasks
        val sourceSets = getSourceSetContainer(target)

        tasks.withType<Test>().configureEach {
            useJUnitPlatform()
            testLogging {
                events("passed", "skipped", "failed")
                showStandardStreams = true
            }
        }

        tasks.register<Test>("executeTestScala213") {
            dependsOn(tasks.named("testScala213Classes"))
            description = "Runs all tests in this module using Scala 2.13"
            group = "verification"
            testClassesDirs = sourceSets["testScala213"].output.classesDirs
            classpath = sourceSets["testScala213"].runtimeClasspath
        }

        val archiveBaseName = getPluginExtension(target).archiveBaseName
        tasks.register<Jar>("scala213Jar").configure {
            dependsOn(tasks.named("scala213Classes"))
            description = "Assembles a jar archive containing the main classes compiled using Scala 2.13 variants of the Apache Spark libraries"
            group = "build"
            from(sourceSets["scala213"].output)
            archiveFileName.set("${archiveBaseName.get()}_2.13-${archiveVersion.get()}.${archiveExtension.get()}")
            includeEmptyDirs = false
        }

        tasks.named("check") {
            dependsOn(tasks.named("executeTestScala213"))
        }

        tasks.named("assemble").configure {
            dependsOn(tasks.named("scala213Jar"), tasks.named("jar"))
        }
    }

    private fun configureArtifacts(target: Project) {
        target.artifacts {
            add("scala213RuntimeElements", target.tasks.named("scala213Jar"))
        }
    }
}
