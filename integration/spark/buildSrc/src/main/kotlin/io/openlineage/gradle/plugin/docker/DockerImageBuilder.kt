/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.docker

import kotlinx.serialization.json.Json
import org.gradle.api.Project

internal class DockerImageBuilder(
    private val p: Project,
    private val ext: DockerBuildPluginExtension
) {

    fun registerTasks() {
        val buildDockerImageTasks = loadManifests().map { manifest ->
            DockerImageBuilderDelegate(p, ext, manifest)
        }.map { delegate ->
            delegate.registerTasks()
        }

        p.tasks.register("buildAllDockerImages") {
            group = "docker"
            description = "Build all Docker images"
            dependsOn(buildDockerImageTasks)
        }
    }

    private fun loadManifests(): List<DockerManifest> {
        val file = ext.manifest.get()
        val text = file.asFile.readText()
        try {
            return Json.decodeFromString<List<DockerManifest>>(text)
        } catch (e: Exception) {
            throw IllegalStateException("Failed to parse the Docker manifest file located here: '${file.asFile.path}'. " +
                    "This could be because of a deserialization issue, or a file access related issue.", e)
        }
    }
}
