import de.undercouch.gradle.tasks.download.Download
import groovy.json.JsonSlurper

plugins {
    id("de.undercouch.download") version "5.5.0"
}

val destDir = layout.buildDirectory.dir("docker")
val downloadDir = layout.projectDirectory.dir("bin")
val manifestJsonFile = layout.projectDirectory.file("manifest.json")
val dockerBuildPlatforms = objects.setProperty(String::class.java)
        .convention(setOf("linux/amd64", "linux/arm64"))
val repository = objects.property(String::class.java)
        .convention("quay.io/openlineage/hive")
val skipPush = providers.gradleProperty("skipPush")
val manifests = loadManifests()

manifests.forEach { manifest ->
    val builder = HiveDockerImageBuilderDelegate(manifest)
    builder.registerTasks()
}

tasks.register("buildAllHiveDockerImages") {
    group = "docker"
    description = "Build all Hive Docker images"
    dependsOn(tasks.withType<BuildDockerImageTask>())
}

fun loadManifests(): List<HiveDockerManifest> {
    val text = manifestJsonFile.asFile.readBytes()
    try {
        val json = JsonSlurper().parse(text) as ArrayList<*>
        return json.map {
            val map = it as Map<*, *>
            HiveDockerManifest(
                    map["hiveVersion"] as String,
                    map["hadoopVersion"] as String,
                    map["tezVersion"] as String
            )
        }
    } catch (e: Exception) {
        throw IllegalStateException(
                "Failed to parse manifest file at: '${manifestJsonFile.asFile.path}'", e
        )
    }
}

data class HiveDockerManifest(
        val hiveVersion: String,
        val hadoopVersion: String,
        val tezVersion: String
)

abstract class BuildDockerImageTask : DefaultTask() {
    init {
        group = "docker"
        description = "Build the Docker image"
    }

    @get:Input abstract val dockerImageName: Property<String>
    @get:Input abstract val dockerImageTag: Property<String>
    @get:Input abstract val hiveVersion: Property<String>
    @get:Input abstract val hadoopVersion: Property<String>
    @get:Input abstract val tezVersion: Property<String>
    @get:InputDirectory abstract val dockerBuildContext: DirectoryProperty
    @get:Input abstract val platforms: SetProperty<String>
    @get:Input abstract val skip: Property<String>

    @TaskAction
    fun buildDockerImage() {
        val contextDir = dockerBuildContext.get().asFile
        val image = "${dockerImageName.get()}:${dockerImageTag.get()}"
        logger.lifecycle("Building Docker image: $image from context: $contextDir")
        logger.lifecycle("Building Docker image: ${dockerImageName.get()}:${dockerImageTag.get()} from context: $contextDir")

        val commandParts = mutableListOf(
                "docker", "buildx", "build",
                "-t", image,
                "--build-arg", "HIVE_VERSION=${hiveVersion.get()}",
                "--build-arg", "HADOOP_VERSION=${hadoopVersion.get()}",
                "--build-arg", "TEZ_VERSION=${tezVersion.get()}"
        )

        if (!skip.isPresent) {
            logger.lifecycle("Pushing to repository")
            commandParts.add("--push")
            commandParts.add("--platform")
            commandParts.add(platforms.get().joinToString(","))
        } else {
            logger.lifecycle("loading to local")
            commandParts.add("--load")
        }

        commandParts.add(".")

        project.exec {
            workingDir = contextDir
            commandLine(commandParts)
        }
    }
}

class HiveDockerImageBuilderDelegate(private val m: HiveDockerManifest) {
    private val hiveVersion = m.hiveVersion
    private val hadoopVersion = m.hadoopVersion
    private val tezVersion = m.tezVersion
    private val taskSuffix = "Hive${hiveVersion.replace(".", "")}"

    fun registerTasks(): TaskProvider<BuildDockerImageTask> {
        val hadoopDownload = registerDownload("Hadoop", hadoopVersion, "hadoop", ".tar.gz")
        val hadoopAsc = registerDownload("HadoopAsc", hadoopVersion, "hadoop", ".tar.gz.asc")
        val hiveDownload = registerDownload("Hive", hiveVersion, "hive", "-bin.tar.gz")
        val hiveAsc = registerDownload("HiveAsc", hiveVersion, "hive", "-bin.tar.gz.asc")
        val tezDownload = registerDownload("Tez", tezVersion, "tez", "-bin.tar.gz")
        val tezAsc = registerDownload("TezAsc", tezVersion, "tez", "-bin.tar.gz.asc")

        val copyDeps = registerCopyTask(hadoopDownload, hadoopAsc, hiveDownload, hiveAsc, tezDownload, tezAsc)
        return registerBuildTask(copyDeps)
    }

    private fun registerDownload(name: String, version: String, component: String, suffix: String): TaskProvider<Download> {
        val fileName = when (component) {
            "hadoop" -> "hadoop-$version$suffix"
            "hive" -> "apache-hive-$version$suffix"
            "tez" -> "apache-tez-$version$suffix"
            else -> throw IllegalArgumentException("Unknown component: $component")
        }

        val baseUrl = when (component) {
            "hadoop" -> "https://archive.apache.org/dist/hadoop/core/hadoop-$version"
            "hive" -> "https://archive.apache.org/dist/hive/hive-$version"
                "tez" -> "https://archive.apache.org/dist/tez/$version"
            else -> ""
        }

        val taskName = "download${name}${taskSuffix}"
        return tasks.register<Download>(taskName) {
            group = "docker"
            src("$baseUrl/$fileName")
            dest(downloadDir.file("hive-${hiveVersion}/$fileName"))
            overwrite(false)
        }
    }

    private fun registerCopyTask(vararg downloadTasks: TaskProvider<Download>): TaskProvider<Task> {
        val taskName = "copyHiveDockerDependencies${taskSuffix}"
        return tasks.register(taskName) {
            dependsOn(downloadTasks.toList())
            group = "docker"
            description = "Copy all files for Hive Docker build context"
            doLast {
                val outputDir = destDir.get().dir("hive-${hiveVersion}").asFile
                outputDir.mkdirs()
                downloadTasks.forEach {
                    val srcFile = it.get().dest
                    project.copy {
                        from(srcFile)
                        into(outputDir)
                    }
                }
                // Also copy Dockerfile
                project.copy {
                    from(layout.projectDirectory.file("."))
                    include("Dockerfile")
                    include("entrypoint.sh")
                    include("conf/**")
                    into(outputDir)
                }
            }
        }
    }

    private fun registerBuildTask(dependency: TaskProvider<Task>): TaskProvider<BuildDockerImageTask> {
        val taskName = "buildHiveDockerImage${taskSuffix}"
        return tasks.register<BuildDockerImageTask>(taskName) {
            dependsOn(dependency)
            group = "docker"
            description = "Build Docker image for Hive $hiveVersion"
            dockerImageName.set(repository)
            dockerImageTag.set(m.hiveVersion)
            hiveVersion.set(m.hiveVersion)
            hadoopVersion.set(m.hadoopVersion)
            tezVersion.set(m.tezVersion)
            dockerBuildContext.set(destDir.map { it.dir("hive-${m.hiveVersion}") })
            platforms.set(dockerBuildPlatforms)
            skip.set(skipPush)
        }
    }
}
