/*
* SPDX-License-Identifier: Apache-2.0
* Copyright 2018-2023 contributors to the OpenLineage project
*/

package io.openlineage.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.kotlin.dsl.withType

/**
 * A Gradle plugin for configuring and managing Scala version-specific variants in a multi-module Java project.
 * This plugin is designed to work with projects that depend on different Scala versions of Apache Spark.
 * It dynamically creates tasks, configurations, and source sets for each defined Scala version,
 * facilitating separate compilation, testing, and packaging for Scala 2.12 and Scala 2.13 variants.
 *
 * Usage:
 * Apply this plugin to your Gradle project and define the Scala variants in the project's build script.
 * The plugin will handle the creation of source sets, configurations, and tasks for each variant.
 *
 * Example:
 * ```
 * scalaVariants {
 *     create("2.12")
 *     create("2.13")
 * }
 * ```
 *
 * Once configured, this plugin allows building and testing the project separately for each Scala version.
 */
class ScalaVariantsPlugin : Plugin<Project> {
    override fun apply(target: Project) {
        target.plugins.withType<JavaPlugin> {
            val objects = target.objects

            val scalaVersionsContainer =
                objects.domainObjectContainer(ScalaBuildVariant::class.java) {
                    objects.newInstance(ScalaBuildVariant::class.java, it)
                }

            target.extensions.add("scalaVariants", scalaVersionsContainer)

            scalaVersionsContainer.all {
                val scalaBinaryVersion = this.name
                val delegate = ScalaVariantDelegate(target, scalaBinaryVersion)
                delegate.configureVariant()
            }
        }
    }
}
