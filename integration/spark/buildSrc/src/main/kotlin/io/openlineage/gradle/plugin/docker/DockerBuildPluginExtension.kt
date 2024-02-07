/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.docker

import org.gradle.api.Project
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty

abstract class DockerBuildPluginExtension @javax.inject.Inject constructor(p: Project) {
    internal val dockerFileTemplate: RegularFileProperty = p.objects.fileProperty().convention(
        p.layout.projectDirectory.file("docker/Dockerfile.template")
    )

    internal val manifestFile: RegularFileProperty = p.objects.fileProperty().convention(
        p.layout.projectDirectory.file("docker/manifest.json")
    )

    internal val downloadDir = p.objects.directoryProperty().convention(
        p.layout.projectDirectory.dir("bin")
    )

    internal val destinationDir = p.objects.directoryProperty().convention(
        p.layout.buildDirectory.dir("docker")
    )

    fun dockerfileTemplate(file: RegularFileProperty) {
        dockerFileTemplate.set(file)
    }

    fun manifestFile(file: RegularFileProperty) {
        manifestFile.set(file)
    }

    fun downloadDir(dir: DirectoryProperty) {
        downloadDir.set(dir)
    }

    fun destinationDir(dir: DirectoryProperty) {
        destinationDir.set(dir)
    }
}
