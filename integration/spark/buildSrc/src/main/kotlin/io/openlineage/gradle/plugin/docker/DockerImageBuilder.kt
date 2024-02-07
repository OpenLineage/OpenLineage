/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.docker

import de.undercouch.gradle.tasks.download.Download
import kotlinx.serialization.json.Json
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.provider.Provider
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.TaskProvider
import org.gradle.kotlin.dsl.register

class DockerImageBuilder(
    private val p: Project,
    private val ext: DockerBuildPluginExtension
) {
    private val tasks = p.tasks

    fun registerTasks() {
        loadManifests().forEach {
            registerTasks(it)
        }

        tasks.register("buildDockerImages") {
            group = "docker"
            description = "Build all Docker images"
            dependsOn(tasks.withType(BuildDockerImageTask::class.java))
        }
    }

    private fun registerTasks(m: DockerManifest) {
        val downloadSparkBinariesTask = registerDownloadSparkBinaries(m)
        val downloadSparkBinariesAscTask = registerDownloadSparkBinariesAsc(m)
        val downloadSparkPgpKeysTask = registerDownloadSparkPgpKeys(m)
        val copySparkBinariesTask = registerCopySparkBinaries(m, downloadSparkBinariesTask)
        val copySparkBinariesAscTask = registerCopySparkBinariesAsc(m, downloadSparkBinariesAscTask)
        val copySparkPgpKeysTask = registerCopySparkPgpKeys(m, downloadSparkPgpKeysTask)
        val copyDependenciesTask = registerCopyDockerDependenciesTask(
            m,
            copySparkBinariesTask,
            copySparkBinariesAscTask,
            copySparkPgpKeysTask
        )
        val createDockerFileTask = registerCreateDockerFileTask(m)
        registerBuildDockerImageTask(m, createDockerFileTask, copyDependenciesTask)
    }

    private fun formatName(m: DockerManifest) = formatName(m.sparkVersion, m.scalaBinaryVersion)

    private fun formatName(sparkVersion: String, scalaBinaryVersion: String) =
        "Spark${sparkVersion.replace(".", "")}Scala${scalaBinaryVersion.replace(".", "")}"

    private fun registerDownloadSparkBinaries(m: DockerManifest): TaskProvider<Download> {
        val taskName = "downloadSparkBinaries${formatName(m)}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkSourceBinaries)
            dest(ext.downloadDir.map { dir ->
                dir.file("spark-${m.sparkVersion}/scala-${m.scalaBinaryVersion}/spark.tgz")
            })
            overwrite(false)
        }
    }

    private fun registerDownloadSparkBinariesAsc(m: DockerManifest): TaskProvider<Download> {
        val taskName = "downloadSparkBinariesAsc${formatName(m)}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkSourceBinariesAsc)
            dest(ext.downloadDir.map { dir ->
                dir.file("spark-${m.sparkVersion}/scala-${m.scalaBinaryVersion}/spark.tgz.asc")
            })
            overwrite(false)
        }
    }

    private fun registerDownloadSparkPgpKeys(m: DockerManifest): TaskProvider<Download> {
        val taskName = "downloadSparkPgpKeys${formatName(m)}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkPgpKeys)
            dest(ext.downloadDir.map { dir ->
                dir.file("spark-${m.sparkVersion}/scala-${m.scalaBinaryVersion}/KEYS")
            })
            overwrite(false)
        }
    }

    private fun registerCopyFileTask(
        taskName: String,
        m: DockerManifest,
        task: TaskProvider<Download>
    ): TaskProvider<Copy> = tasks.register<Copy>(taskName) {
        dependsOn(task)
        group = "docker"
        this.description = description
        from(task.map { it.dest })
        into(ext.destinationDir.map { dir ->
            dir.dir("spark-${m.sparkVersion}/scala-${m.scalaBinaryVersion}")
        })
    }

    private fun registerCopySparkBinaries(
        m: DockerManifest,
        task: TaskProvider<Download>
    ): TaskProvider<Copy> {
        val taskName = "copySparkBinaries${formatName(m)}"
        return registerCopyFileTask(taskName, m, task)
    }

    private fun registerCopySparkBinariesAsc(
        m: DockerManifest,
        task: TaskProvider<Download>
    ): TaskProvider<Copy> {
        val taskName = "copySparkBinariesAsc${formatName(m)}"
        return registerCopyFileTask(taskName, m, task)
    }

    private fun registerCopySparkPgpKeys(
        m: DockerManifest,
        task: TaskProvider<Download>
    ): TaskProvider<Copy> {
        val taskName = "copySparkPgpKeys${formatName(m)}"
        return registerCopyFileTask(taskName, m, task)
    }

    private fun registerCopyDockerDependenciesTask(
        m: DockerManifest,
        binariesTask: TaskProvider<Copy>,
        ascTask: TaskProvider<Copy>,
        pgpKeysTask: TaskProvider<Copy>
    ): TaskProvider<Task> {
        val taskName = "copyDockerDependencies${formatName(m)}"
        return tasks.register(taskName) {
            dependsOn(binariesTask, ascTask, pgpKeysTask)
            group = "docker"
            description =
                "Aggregate task that triggers the copying of all necessary files " +
                        "for the Docker Image for Spark ${m.sparkVersion} and " +
                        "Scala ${m.scalaBinaryVersion} to be built"
        }
    }

    private fun registerCreateDockerFileTask(m: DockerManifest): TaskProvider<MaterialiseDockerfileTask> {
        val taskName = "createDockerFile${formatName(m)}"
        return tasks.register<MaterialiseDockerfileTask>(taskName) {
            baseDockerImage(m.baseDockerImage)
            templateFile.set(ext.dockerFileTemplate)
            destinationFile.set(
                ext.destinationDir.map { dir ->
                    dir.file("spark-${m.sparkVersion}/scala-${m.scalaBinaryVersion}/Dockerfile")
                }
            )
        }
    }

    private fun registerBuildDockerImageTask(
        m: DockerManifest,
        materialiseDockerFileTask: TaskProvider<MaterialiseDockerfileTask>,
        copyDependenciesTask: TaskProvider<Task>
    ): Provider<BuildDockerImageTask> {
        val taskName = "buildDockerImage${formatName(m)}"
        return tasks.register<BuildDockerImageTask>(taskName) {
            dependsOn(materialiseDockerFileTask, copyDependenciesTask)
            dockerImageName.set("openlineage/spark")
            dockerImageTag.set("spark-${m.sparkVersion}-scala-${m.scalaBinaryVersion}")
            dockerBuildContext.set(ext.destinationDir.map { dir ->
                dir.dir("spark-${m.sparkVersion}/scala-${m.scalaBinaryVersion}")
            })
        }
    }


    private fun loadManifests(): List<DockerManifest> {
        val file = ext.manifestFile.get()
        val text = file.asFile.readText()
        return Json.decodeFromString<List<DockerManifest>>(text)
    }
}
