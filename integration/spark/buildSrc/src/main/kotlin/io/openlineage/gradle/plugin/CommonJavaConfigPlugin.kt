/*
* SPDX-License-Identifier: Apache-2.0
* Copyright 2018-2023 contributors to the OpenLineage project
*/

package io.openlineage.gradle.plugin

import org.gradle.api.JavaVersion
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPluginExtension
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.maven

/**
 * A Gradle plugin that applies common Java configurations to the project.
 *
 * This plugin applies the "java-library" plugin, sets the source and target compatibility to Java 8,
 * and configures the project to use Maven Central, Maven Local, and a custom Astronomer repository.
 *
 * To apply this plugin, add the following to your `build.gradle.kts`:
 *
 * ```kotlin
 * plugins {
 *     id("io.openlineage.common-java-config")
 * }
 * ```
 *
 * @see org.gradle.api.Plugin
 */
class CommonJavaConfigPlugin : Plugin<Project> {
    override fun apply(target: Project) {
        configurePlugins(target)
        configureJava(target)
        configureRepositories(target)
    }

    private fun configurePlugins(target: Project) {
        target.plugins.apply("java-library")
    }

    private fun configureJava(target: Project) {
        target.extensions.getByType<JavaPluginExtension>().apply {
            sourceCompatibility = JavaVersion.VERSION_1_8
            targetCompatibility = JavaVersion.VERSION_1_8
        }
    }

    private fun configureRepositories(target: Project) {
        target.repositories.mavenCentral()
        target.repositories.mavenLocal()
        target.repositories.maven("https://astronomer.jfrog.io/artifactory/maven-public-libs-snapshot")
    }
}
