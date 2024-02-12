/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaLibraryPlugin

class BuildVariantPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = with(project) {
        plugins.withType(JavaLibraryPlugin::class.java) {
            extensions.create("buildVariants", VariantPluginExtension::class.java)
        }
    }
}
