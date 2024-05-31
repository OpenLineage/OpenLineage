import de.undercouch.gradle.tasks.download.Download
import groovy.json.JsonSlurper

plugins {
    id("de.undercouch.download") version "5.5.0"
}

val destDir = layout.buildDirectory.dir("docker")
val dockerfile = layout.projectDirectory.file("Dockerfile")
val downloadDir = layout.projectDirectory.dir("bin")
val manifestJsonFile = layout.projectDirectory.file("manifest.json")
val dockerBuildPlatforms =
    objects.setProperty(String::class.java).convention(setOf("linux/amd64", "linux/arm64"))
val repository = objects.property(String::class.java).convention("openlineage/spark")

val manifests = loadManifests()

manifests.forEach { manifest ->
    val builder = DockerImageBuilderDelegate(manifest)
    builder.registerTasks()
}

tasks.register("buildAllDockerImages") {
    group = "docker"
    description = "Build all Docker images"
    dependsOn(tasks.withType<BuildDockerImageTask>())
}

fun loadManifests(): List<DockerManifest> {
    val text = manifestJsonFile.asFile.readBytes()
    try {
        val json = JsonSlurper().parse(text) as ArrayList<*>
        return json.map {
            val map = it as Map<*, *>
            DockerManifest(
                map["baseImageTag"] as String,
                map["scalaBinaryVersion"] as String,
                map["sparkPgpKeys"] as String,
                map["sparkSourceBinaries"] as String,
                map["sparkSourceBinariesAsc"] as String,
                map["sparkVersion"] as String,
            )
        }
    } catch (e: Exception) {
        throw IllegalStateException(
            "Failed to parse the Docker manifest file located here: '${manifestJsonFile.asFile.path}'. " +
                    "This could be because of a deserialization issue, or a file access related issue.",
            e
        )
    }
}


abstract class BuildDockerImageTask : DefaultTask() {
    init {
        group = "docker"
        description = "Build the Docker image"
    }

    @get:Input
    abstract val baseImageTag: Property<String>

    @get:Input
    abstract val dockerImageName: Property<String>

    @get:Input
    abstract val dockerImageTag: Property<String>

    @get:InputDirectory
    abstract val dockerBuildContext: DirectoryProperty

    @get:Input
    abstract val platforms: SetProperty<String>

    @TaskAction
    fun buildDockerImage() {
        val dockerBuildContextDir = dockerBuildContext.get().asFile
        val dockerImage = "${dockerImageName.get()}:${dockerImageTag.get()}"
        logger.lifecycle("Building Docker image $dockerImage from $dockerBuildContextDir")
        project.exec {
            workingDir = dockerBuildContextDir
            val commandParts = listOf(
                "docker",
                "buildx",
                "build",
                "--platform",
                platforms.get().joinToString(","),
                "-t",
                dockerImage,
                "--build-arg",
                "BASE_IMAGE_TAG=${baseImageTag.get()}",
                "--push",
                "."
            )
            logger.lifecycle("Issuing Docker command: {}", commandParts.joinToString(" "))
            commandLine(*commandParts.toTypedArray())
        }
    }
}

class DockerImageBuilderDelegate(private val m: DockerManifest) {
    private val sparkVersion = m.sparkVersion
    private val scalaBinaryVersion = m.scalaBinaryVersion
    private val taskSuffix = formatTaskSuffix()

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
            dest(downloadDir.file("spark-${sparkVersion}/scala-${scalaBinaryVersion}/spark.tgz"))
            overwrite(false)
        }
    }

    private fun registerDownloadSparkBinariesAsc(): TaskProvider<Download> {
        val taskName = "downloadSparkBinariesAsc${taskSuffix}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkSourceBinariesAsc)
            dest(downloadDir.file("spark-${sparkVersion}/scala-${scalaBinaryVersion}/spark.tgz.asc"))
            overwrite(false)
        }
    }

    private fun registerDownloadSparkPgpKeys(): TaskProvider<Download> {
        val taskName = "downloadSparkPgpKeys${taskSuffix}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src(m.sparkPgpKeys)
            dest(downloadDir.file("spark-${sparkVersion}/scala-${scalaBinaryVersion}/KEYS"))
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
        into(destDir.map { dir ->
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
            description =
                "Copy the Dockerfile for Spark $sparkVersion and Scala $scalaBinaryVersion"
            from(dockerfile)
            into(destDir.map { dir ->
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
            description =
                "Build the Docker Image for Spark $sparkVersion and Scala $scalaBinaryVersion"
            baseImageTag.set(m.baseImageTag)
            dockerImageName.set(repository)
            dockerImageTag.set("spark-${sparkVersion}-scala-${scalaBinaryVersion}")
            dockerBuildContext.set(destDir.map { dir ->
                dir.dir("spark-${sparkVersion}/scala-${scalaBinaryVersion}")
            })
            platforms.set(dockerBuildPlatforms)
        }
    }
}

data class DockerManifest(
    val baseImageTag: String,
    val scalaBinaryVersion: String,
    val sparkPgpKeys: String,
    val sparkSourceBinaries: String,
    val sparkSourceBinariesAsc: String,
    val sparkVersion: String,
)
