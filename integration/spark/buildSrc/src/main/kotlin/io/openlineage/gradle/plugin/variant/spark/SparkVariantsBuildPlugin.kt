/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaPlugin
import org.gradle.kotlin.dsl.withType

class SparkVariantsBuildPlugin : Plugin<Project> {
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
