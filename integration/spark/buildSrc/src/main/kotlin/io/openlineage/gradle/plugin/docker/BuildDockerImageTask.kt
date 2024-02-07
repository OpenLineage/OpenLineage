/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.docker

import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.TaskAction

abstract class BuildDockerImageTask : DefaultTask() {

    init {
        group = "docker"
        description = "Build the Docker image"
    }

    @get:Input
    abstract val dockerImageName: Property<String>

    @get:Input
    abstract val dockerImageTag: Property<String>

    @get:InputDirectory
    abstract val dockerBuildContext: DirectoryProperty

    @TaskAction
    fun buildDockerImage() {
        val dockerBuildContextDir = dockerBuildContext.get().asFile
        val dockerImage = "${dockerImageName.get()}:${dockerImageTag.get()}"
        logger.lifecycle("Building Docker image $dockerImage from $dockerBuildContextDir")
        project.exec {
            workingDir = dockerBuildContextDir
            commandLine(
                "docker",
                "build",
                "-t",
                dockerImage,
                "."
            )
        }
    }

}
