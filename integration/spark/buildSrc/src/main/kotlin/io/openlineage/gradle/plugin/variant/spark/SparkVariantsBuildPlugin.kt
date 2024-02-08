/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.kotlin.dsl.withType

/**
 * This Gradle plugin enables the construction of source sets and tasks
 * for multiple variants of Apache Spark and Scala versions. It aids in creating a unified build process
 * that compiles and tests code across different Spark and Scala versions, mitigating runtime failures
 * due to version discrepancies. This plugin operates within projects that apply the Java plugin.
 *
 * The plugin offers several key features:
 * - Generation of variant-specific source sets for both main and test sources, named according to
 *   the pattern 'mainSpark${sparkVersion}Scala${scalaBinaryVersion}' and
 *   'testSpark${sparkVersion}Scala${scalaBinaryVersion}', with periods in versions removed.
 * - Creation of aggregate tasks for compiling main and test sources, running unit and integration tests,
 *   as well as packaging compiled code into JARs, including shadow JARs for creating fat JARs with all dependencies.
 * - A utility task for printing the details of the created source sets.
 *
 * To leverage the plugin, a consumer must define the desired Apache Spark version, Scala Binary version,
 * and a build name through the provided extension. This setup allows for compiling and testing against different
 * versions, facilitating a broader compatibility range and reducing the potential for version-related runtime issues.
 */
class SparkVariantsBuildPlugin : Plugin<Project> {
    /**
     * Applies the SparkVariantsBuildPlugin to the given project. This method sets up the required tasks and extensions
     * when the Java plugin is applied to the project.
     *
     * Specifically, it:
     * - Creates an extension of type [SparkVariantsBuildPluginExtension] for configuring the plugin.
     * - Registers tasks for compiling main and test sources, running unit and integration tests,
     *   assembling JARs, including shadow JARs, and printing details of generated source sets.
     *
     * The functionality is only initiated if the Java plugin is already applied to the project, ensuring
     * the necessary Java compilation, packaging, and testing infrastructure is in place.
     *
     * @param p The project to which the SparkVariantsBuildPlugin is applied.
     */
    override fun apply(p: Project) {
        p.plugins.withType<JavaPlugin> {
            p.extensions.create(
                "builds", SparkVariantsBuildPluginExtension::class.java
            )

            p.registerAggregateCompileMainSourcesTask()
            p.registerAggregateCompileTestSourcesTask()
            p.registerAggregateUnitTestTask()
            p.registerAggregateJarTask()
            p.registerAggregateIntegrationTestTask()
            p.registerAggregateShadowJarTasks()

            p.registerPrintSourceSetDetailsTasks()
        }
    }
}
