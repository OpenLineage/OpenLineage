/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.docker

import org.gradle.api.Plugin
import org.gradle.api.Project

class DockerBuildPlugin : Plugin<Project> {
    override fun apply(p: Project) {
        val extension = p.extensions.create("docker", DockerBuildPluginExtension::class.java)
        val builder = DockerImageBuilder(p, extension)
        builder.registerTasks()
    }
}

