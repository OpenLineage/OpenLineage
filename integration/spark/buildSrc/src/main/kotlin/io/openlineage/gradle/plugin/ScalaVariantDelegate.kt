/**
 * Copyright 2018-2025 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin

import org.gradle.api.Project
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.tasks.Jar
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.register

/**
 * A delegate class used by the ScalaVariantsPlugin to configure a specific Scala variant.
 * It encapsulates the logic for setting up source sets, configurations, tasks, and artifacts
 * for a given Scala version within a Gradle project. This class is responsible for the detailed
 * configuration and wiring required to support building and testing against different Scala versions.
 *
 * The delegate is instantiated for each Scala version defined in the ScalaVariantsPlugin,
 * handling the unique requirements and dependencies for that specific variant.
 *
 * The main responsibilities include:
 * - Creating and configuring source sets for main and test code specific to the Scala version.
 * - Defining and extending configurations for variant-specific dependencies.
 * - Registering tasks for compiling, testing, and packaging the variant.
 * - Setting up appropriate artifacts for the variant.
 *
 * Note:
 * This class is an internal implementation detail of the ScalaVariantsPlugin and is typically
 * not used directly in a project's build script.
 */
class ScalaVariantDelegate(
    private val target: Project,
    private val scalaBinaryVersion: String
) {
    private val archiveName =
        "openlineage-spark-${target.name}_${scalaBinaryVersion}-${target.version}.jar"
    private val sourceSetName = "scala$scalaBinaryVersion".replace(".", "")
    private val testSourceSetName = "test${sourceSetName.capitalized()}"

    fun configureVariant() {
        configureSourceSets()
        configureConfigurations()
        configureTasks()
        configureArtifacts()
    }

    fun CharSequence.capitalized(): String =
        toString().replaceFirstChar { if (it.isLowerCase()) it.titlecase() else it.toString() }

    private fun getSourceSetContainer(target: Project) =
        target.extensions.getByType<SourceSetContainer>()

    private fun configureSourceSets() {
        val sourceSetContainer = target.extensions.getByType<SourceSetContainer>()
        val sourceSet = sourceSetContainer.create(sourceSetName) {
            java.setSrcDirs(listOf("src/main/java", "src/${sourceSetName}/java"))
            resources.setSrcDirs(listOf("src/main/resources", "src/${sourceSetName}/resources"))
        }
        sourceSetContainer.create(testSourceSetName) {
            compileClasspath += sourceSet.output
            runtimeClasspath += sourceSet.output
            java.setSrcDirs(listOf("src/test/java", "src/${testSourceSetName}/java"))
            resources.setSrcDirs(listOf("src/test/resources", "src/${testSourceSetName}/resources"))
        }
    }

    private fun configureConfigurations() {
        val configurations = target.configurations

        val api = configurations.create("${sourceSetName}Api")

        configurations.create("${sourceSetName}ApiElements") {
            extendsFrom(api)
            isCanBeResolved = false
            isCanBeConsumed = true
        }

        val implementation = configurations.named("${sourceSetName}Implementation") {
            extendsFrom(api)
        }

        val runtimeOnly = configurations.named("${sourceSetName}RuntimeOnly")
        val compileOnly = configurations.named("${sourceSetName}CompileOnly")

        configurations.create("${sourceSetName}RuntimeElements") {
            extendsFrom(implementation.get(), runtimeOnly.get())
            isCanBeResolved = false
            isCanBeConsumed = true
        }

        configurations.named("${sourceSetName}CompileClasspath") {
            extendsFrom(compileOnly.get(), implementation.get())
            isCanBeResolved = true
        }

        configurations.named("${sourceSetName}RuntimeClasspath") {
            extendsFrom(implementation.get(), runtimeOnly.get())
            isCanBeResolved = true
        }

        val testImplementation = configurations.named("${testSourceSetName}Implementation") {
            extendsFrom(implementation.get())
        }

        val testRuntimeOnly = configurations.named("${testSourceSetName}RuntimeOnly") {
            extendsFrom(runtimeOnly.get())
        }

        val testCompileOnly = configurations.named("${testSourceSetName}CompileOnly")

        configurations.named("${testSourceSetName}CompileClasspath") {
            extendsFrom(testCompileOnly.get(), testImplementation.get())
            isCanBeResolved = true
        }

        configurations.named("${testSourceSetName}RuntimeClasspath") {
            extendsFrom(testImplementation.get(), testRuntimeOnly.get())
            isCanBeResolved = true
        }
    }

    private fun configureTasks() {
        val tasks = target.tasks
        val sourceSets = getSourceSetContainer(target)

        tasks.register<Test>("execute${testSourceSetName.capitalized()}") {
            dependsOn(tasks.named("${testSourceSetName}Classes"))
            description = "Runs all tests in this module using Scala $scalaBinaryVersion"
            group = "verification"
            testClassesDirs = sourceSets[testSourceSetName].output.classesDirs
            classpath = sourceSets[testSourceSetName].runtimeClasspath
        }

        tasks.register<Jar>("${sourceSetName}Jar").configure {
            dependsOn(tasks.named("${sourceSetName}Classes"))
            description =
                "Assembles a jar archive containing the main classes compiled using Scala $scalaBinaryVersion variants of the Apache Spark libraries"
            group = "build"
            from(sourceSets[sourceSetName].output)
            destinationDirectory.set(target.file(target.layout.buildDirectory.file("libs/$sourceSetName")))
            archiveFileName.set(archiveName)
            includeEmptyDirs = false
        }

        tasks.named("check") {
            dependsOn(tasks.named("execute${testSourceSetName.capitalized()}"))
        }

        tasks.named("assemble").configure {
            dependsOn(tasks.named("${sourceSetName}Jar"))
        }
    }

    private fun configureArtifacts() {
        target.artifacts {
            add("${sourceSetName}RuntimeElements", target.tasks.named("${sourceSetName}Jar"))
        }
    }
}
