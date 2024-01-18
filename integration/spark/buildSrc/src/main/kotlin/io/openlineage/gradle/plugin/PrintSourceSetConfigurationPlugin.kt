/*
* SPDX-License-Identifier: Apache-2.0
* Copyright 2018-2023 contributors to the OpenLineage project
*/

package io.openlineage.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSetContainer

/**
 * A Gradle plugin that registers a task named `printSourceSetConfiguration` to the project.
 *
 * This task, when executed, prints the configuration of the source sets in the project. This includes the name of each source set, its source directories, output directories, and compile classpath. This plugin can be useful for debugging and understanding how the source sets are configured in a project.
 *
 * To apply this plugin, add the following to your `build.gradle.kts`:
 *
 * ```kotlin
 * plugins {
 *     id("io.openlineage.print-source-set-configuration")
 * }
 * ```
 *
 * To execute the `printSourceSetConfiguration` task, run the following command in your terminal:
 *
 * ```bash
 * ./gradlew printSourceSetConfiguration
 * ```
 *
 * @see org.gradle.api.Plugin
 */
class PrintSourceSetConfigurationPlugin : Plugin<Project> {
    override fun apply(target: Project) {
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
