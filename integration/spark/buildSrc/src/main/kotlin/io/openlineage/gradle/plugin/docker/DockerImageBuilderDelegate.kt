package io.openlineage.gradle.plugin.docker

import de.undercouch.gradle.tasks.download.Download
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.TaskProvider
import org.gradle.kotlin.dsl.register

internal class DockerImageBuilderDelegate(
    p: Project,
    private val ext: DockerBuildPluginExtension,
    private val m: DockerManifest
) {
    private val tasks = p.tasks
    private val taskSuffix = formatTaskSuffix()
    private val sparkVersion by m::sparkVersion
    private val scalaBinaryVersion by m::scalaBinaryVersion


    fun registerTasks(): TaskProvider<BuildDockerImageTask> {
        val downloadSparkBinariesTask = registerDownloadSparkBinaries()
        val downloadSparkBinariesAscTask = registerDownloadSparkBinariesAsc()
        val downloadSparkPgpKeysTask = registerDownloadSparkPgpKeys()
        val copySparkBinariesTask = registerCopySparkBinaries(downloadSparkBinariesTask)
        val copySparkBinariesAscTask = registerCopySparkBinariesAsc(downloadSparkBinariesAscTask)
        val copySparkPgpKeysTask = registerCopySparkPgpKeys(downloadSparkPgpKeysTask)
        val copyDockerfileTask = registerCopyDockerfileTask()
        val copyDependenciesTask = registerCopyDockerDependenciesTask(
            copySparkBinariesTask,
            copySparkBinariesAscTask,
            copySparkPgpKeysTask,
            copyDockerfileTask,
        )
        return registerBuildDockerImageTask(copyDependenciesTask)
    }


    private fun formatTaskSuffix() = formatTaskSuffix(sparkVersion, scalaBinaryVersion)

    private fun formatTaskSuffix(sparkVersion: String, scalaBinaryVersion: String) =
        "Spark${sparkVersion.replace(".", "")}Scala${scalaBinaryVersion.replace(".", "")}"

    private fun registerDownloadSparkBinaries(): TaskProvider<Download> {
        val taskName = "downloadSparkBinaries${taskSuffix}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkSourceBinaries)
            dest(ext.downloadDir.map { dir ->
                dir.file("spark-${sparkVersion}/scala-${scalaBinaryVersion}/spark.tgz")
            })
            overwrite(false)
        }
    }

    private fun registerDownloadSparkBinariesAsc(): TaskProvider<Download> {
        val taskName = "downloadSparkBinariesAsc${taskSuffix}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkSourceBinariesAsc)
            dest(ext.downloadDir.map { dir ->
                dir.file("spark-${sparkVersion}/scala-${scalaBinaryVersion}/spark.tgz.asc")
            })
            overwrite(false)
        }
    }

    private fun registerDownloadSparkPgpKeys(): TaskProvider<Download> {
        val taskName = "downloadSparkPgpKeys${taskSuffix}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkPgpKeys)
            dest(ext.downloadDir.map { dir ->
                dir.file("spark-${sparkVersion}/scala-${scalaBinaryVersion}/KEYS")
            })
            overwrite(false)
        }
    }

    private fun registerCopyFileTask(
        taskName: String,
        task: TaskProvider<Download>
    ): TaskProvider<Copy> = tasks.register<Copy>(taskName) {
        dependsOn(task)
        group = "docker"
        this.description = description
        from(task.map { it.dest })
        into(ext.destinationDir.map { dir ->
            dir.dir("spark-${sparkVersion}/scala-${scalaBinaryVersion}")
        })
    }

    private fun registerCopySparkBinaries(
        task: TaskProvider<Download>
    ): TaskProvider<Copy> {
        val taskName = "copySparkBinaries${taskSuffix}"
        return registerCopyFileTask(taskName, task)
    }

    private fun registerCopySparkBinariesAsc(
        task: TaskProvider<Download>
    ): TaskProvider<Copy> {
        val taskName = "copySparkBinariesAsc${taskSuffix}"
        return registerCopyFileTask(taskName, task)
    }

    private fun registerCopySparkPgpKeys(
        task: TaskProvider<Download>
    ): TaskProvider<Copy> {
        val taskName = "copySparkPgpKeys${taskSuffix}"
        return registerCopyFileTask(taskName, task)
    }

    private fun registerCopyDockerfileTask(): TaskProvider<Copy> {
        val taskName = "copyDockerfile${taskSuffix}"
        return tasks.register<Copy>(taskName) {
            group = "docker"
            description = "Copy the Dockerfile for Spark $sparkVersion and Scala $scalaBinaryVersion"
            from(ext.dockerfile)
            into(ext.destinationDir.map { dir ->
                dir.dir("spark-${sparkVersion}/scala-${scalaBinaryVersion}")
            })
        }
    }

    private fun registerCopyDockerDependenciesTask(
        vararg copyDependenciesTask: TaskProvider<Copy>
    ): TaskProvider<Task> {
        val taskName = "copyDockerDependencies${taskSuffix}"
        return tasks.register(taskName) {
            dependsOn(copyDependenciesTask.toList())
            group = "docker"
            description =
                "Aggregate task that triggers the copying of all necessary files " +
                        "for the Docker Image for Spark $sparkVersion and " +
                        "Scala $scalaBinaryVersion to be built"
        }
    }

    private fun registerBuildDockerImageTask(dependency: TaskProvider<Task>): TaskProvider<BuildDockerImageTask> {
        val taskName = "buildDockerImage${taskSuffix}"
        return tasks.register<BuildDockerImageTask>(taskName) {
            dependsOn(dependency)
            group = "docker"
            description = "Build the Docker Image for Spark $sparkVersion and Scala $scalaBinaryVersion"
            baseImageTag.set(m.baseImageTag)
            dockerImageName.set("openlineage/spark")
            dockerImageTag.set("spark-${sparkVersion}-scala-${scalaBinaryVersion}")
            dockerBuildContext.set(ext.destinationDir.map { dir ->
                dir.dir("spark-${sparkVersion}/scala-${scalaBinaryVersion}")
            })
        }
    }
}