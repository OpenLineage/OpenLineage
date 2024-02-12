/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.docker

import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction

abstract class MaterialiseDockerfileTask : DefaultTask() {
    private var baseDockerImage: String? = null

    init {
        group = "docker"
        description = "Materialise the Dockerfile from the template"
    }

    @get:InputFile
    abstract val templateFile: RegularFileProperty

    @get:OutputFile
    abstract val destinationFile: RegularFileProperty

    fun baseDockerImage(baseDockerImage: String) {
        this.baseDockerImage = baseDockerImage
    }

    @TaskAction
    fun materialiseDockerfile() {
        check(baseDockerImage != null) { "baseDockerImage must be set" }
        val rawTemplate = loadDockerfileTemplate()
        val appliedTemplate =
            rawTemplate.replace("##BASE_DOCKER_IMAGE##", baseDockerImage!!)
        val f = destinationFile.get()
        f.asFile.writeText(appliedTemplate)
    }

    private fun loadDockerfileTemplate(): String = templateFile.get().asFile.readText()
}
