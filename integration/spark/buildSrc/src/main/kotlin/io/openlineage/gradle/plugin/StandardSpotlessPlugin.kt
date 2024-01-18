/*
* SPDX-License-Identifier: Apache-2.0
* Copyright 2018-2023 contributors to the OpenLineage project
*/

package io.openlineage.gradle.plugin

import com.diffplug.gradle.spotless.SpotlessExtension
import com.diffplug.spotless.FormatterFunc
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.kotlin.dsl.configure

/**
 * A Gradle plugin that applies the Spotless code formatter to the project.
 *
 * This plugin is configured to use the Google Java Format and to disallow wildcard imports.
 *
 * To apply this plugin, add the following to your `build.gradle.kts`:
 *
 * ```kotlin
 * plugins {
 *     id("io.openlineage.standard-spotless")
 * }
 * ```
 *
 * @see org.gradle.api.Plugin
 */
class StandardSpotlessPlugin : Plugin<Project> {
    override fun apply(target: Project) {
        target.plugins.apply("com.diffplug.spotless")

        val disallowWildcardImports = FormatterFunc { text ->
            val regex = Regex("^import\\s+\\w+(\\.\\w+)*\\.*;$")
            val m = regex.find(text)
            if (m != null) {
                throw RuntimeException("Wildcard imports are disallowed - ${m.groupValues}")
            }
            text
        }

        target.extensions.configure<SpotlessExtension> {
            java {
                googleJavaFormat()
                removeUnusedImports()
                custom("disallowWildcardImports", disallowWildcardImports)
            }
        }
    }
}
