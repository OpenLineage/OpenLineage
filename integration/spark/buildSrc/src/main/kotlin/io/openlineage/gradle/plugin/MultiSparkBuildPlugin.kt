package io.openlineage.gradle.plugin

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import de.undercouch.gradle.tasks.download.Download
import org.gradle.api.DomainObjectCollection
import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.artifacts.ExternalModuleDependency
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.model.ObjectFactory
import org.gradle.api.plugins.JavaPlugin
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.tasks.Jar
import org.gradle.kotlin.dsl.*
import java.util.*
import javax.inject.Inject

class MultiSparkBuildPlugin : Plugin<Project> {
    override fun apply(target: Project) {
        target.plugins.withType<JavaPlugin> {
            val extension = target.extensions.create(
                "sparkVersions",
                MultiSparkBuildPluginExtension::class.java
            )
            val delegate = Delegate(target, extension)
            delegate.configureProject()
        }
    }
}

class Delegate(private val p: Project, private val ext: MultiSparkBuildPluginExtension) {
    fun configureProject() {
        configureSourceSets()
        configureConfigurations()
        configureDependencies()
        configureTasks()
    }

    private fun configureSourceSets() {
        val sourceSetContainer = p.extensions.getByType(SourceSetContainer::class.java)
        ext.sparkBuilds.all {
            val sparkFmt = sparkVersion.replace(".", "")
            scalaBinaryVersions.forEach { scalaBinaryVersion ->
                val scalaFmt = scalaBinaryVersion.replace(".", "")

                val sourceSetName = "spark${sparkFmt}Scala${scalaFmt}"
                val testSourceSetName = "testSpark${sparkFmt}Scala${scalaFmt}"

                sourceSetContainer.create(sourceSetName) {
                    java.setSrcDirs(listOf("src/main/java"))
                    resources.setSrcDirs(listOf("src/main/resources"))
                }

                sourceSetContainer.create(testSourceSetName) {
                    java.setSrcDirs(listOf("src/test/java"))
                    resources.setSrcDirs(listOf("src/test/resources"))

                    runtimeClasspath += sourceSetContainer[sourceSetName].output
                    compileClasspath += sourceSetContainer[sourceSetName].output
                }
            }
        }
    }

    private fun configureConfigurations() {
        val cfg = p.configurations

        ext.sparkBuilds.all {
            val sparkFmt = sparkVersion.replace(".", "")
            scalaBinaryVersions.forEach { scalaBinaryVersion ->
                val scalaFmt = scalaBinaryVersion.replace(".", "")

                val compileOnlyName = "spark${sparkFmt}Scala${scalaFmt}CompileOnly"
                val implName = "spark${sparkFmt}Scala${scalaFmt}Implementation"
                val testCompileOnlyName = "testSpark${sparkFmt}Scala${scalaFmt}CompileOnly"
                val testImplName = "testSpark${sparkFmt}Scala${scalaFmt}Implementation"

                cfg.named(testCompileOnlyName) {
                    extendsFrom(cfg.getByName(compileOnlyName))
                }

                cfg.named(testImplName) {
                    extendsFrom(cfg.getByName(implName))
                }
            }
        }
    }

    private fun configureDependencies() {
        val deps = p.dependencies

        ext.sparkBuilds.configureEach {
            val sparkFmt = sparkVersion.replace(".", "")
            val sparkSeries = sparkVersion.substringBeforeLast(".")

            scalaBinaryVersions.forEach { scalaBinaryVersion ->
                val scalaFmt = scalaBinaryVersion.replace(".", "")
                val compileOnly = "spark${sparkFmt}Scala${scalaFmt}CompileOnly"
                val implName = "spark${sparkFmt}Scala${scalaFmt}Implementation"
                val testImpl = "testSpark${sparkFmt}Scala${scalaFmt}Implementation"

                deps.add(
                    implName,
                    deps.project(
                        ":shared",
                        configuration = "scala${scalaFmt}RuntimeElements"
                    )
                )
                deps.add(
                    implName,
                    deps.project(":spark2", configuration = "scala212RuntimeElements")
                )
                deps.add(
                    implName,
                    deps.project(
                        ":spark3",
                        configuration = "scala${scalaFmt}RuntimeElements"
                    )
                )
                deps.add(
                    implName,
                    deps.project(":spark31", configuration = "scala212RuntimeElements")
                )
                deps.add(
                    implName,
                    deps.project(
                        ":spark32",
                        configuration = "scala${scalaFmt}RuntimeElements"
                    )
                )
                deps.add(
                    implName,
                    deps.project(
                        ":spark33",
                        configuration = "scala${scalaFmt}RuntimeElements"
                    )
                )
                deps.add(
                    implName,
                    deps.project(
                        ":spark34",
                        configuration = "scala${scalaFmt}RuntimeElements"
                    )
                )
                deps.add(
                    implName,
                    deps.project(
                        ":spark35",
                        configuration = "scala${scalaFmt}RuntimeElements"
                    )
                )
                deps.add(
                    implName,
                    "org.apache.httpcomponents.client5:httpclient5:5.3"
                )

                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "org.apache.spark:spark-core_$scalaBinaryVersion:$sparkVersion"
                )
                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "org.apache.spark:spark-sql_$scalaBinaryVersion:$sparkVersion"
                )
                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "org.apache.spark:spark-hive_$scalaBinaryVersion:$sparkVersion"
                )

                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "org.apache.iceberg",
                    "iceberg-spark-runtime-${sparkSeries}_$scalaBinaryVersion",
                    icebergVersion
                )

                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "io.delta",
                    "delta-core_$scalaBinaryVersion",
                    deltaVersion
                )

                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "net.snowflake",
                    "spark-snowflake_$scalaBinaryVersion",
                    snowflakeVersion
                ) {
                    exclude(group = "com.google.guava:guava")
                    exclude(group = "org.apache.spark:spark-core_${scalaBinaryVersion}")
                    exclude(group = "org.apache.spark:spark-sql_${scalaBinaryVersion}")
                    exclude(group = "org.apache.spark:spark-catalyst_${scalaBinaryVersion}")
                }

                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "com.google.cloud.spark",
                    "spark-bigquery-with-dependencies_$scalaBinaryVersion",
                    bigqueryVersion
                ) {
                    exclude(group = "com.fasterxml.jackson.core")
                    exclude(group = "com.fasterxml.jackson.module")
                    exclude(group = "com.sun.jmx")
                    exclude(group = "com.sun.jdmk")
                    exclude(group = "javax.jms")
                }

                deps.addMulti(
                    listOf(compileOnly, testImpl),
                    "org.apache.hadoop",
                    "hadoop-client",
                    hadoopClientVersion
                ) {
                    exclude(group = "org.apache.hadoop", module = "hadoop-hdfs-client")
                    exclude(group = "org.apache.hadoop", module = "hadoop-client")
                    exclude(group = "org.apache.hadoop", module = "hadoop-mapreduce-client-core")
                    exclude(group = "org.apache.hadoop", module = "hadoop-yarn-common")
                    exclude(group = "com.fasterxml.jackson.core")
                }

                deps.add(
                    testImpl,
                    "com.databricks:databricks-sdk-java:0.4.0"
                ) {
                    exclude(group = "com.fasterxml.jackson.core")
                    exclude(group = "com.fasterxml.jackson.module")
                }

                deps.add(
                    testImpl,
                    "org.apache.spark:spark-sql-kafka-0-10_$scalaBinaryVersion:$sparkVersion"
                )

                deps.add(
                    testImpl,
                    deps.platform("org.junit:junit-bom:5.10.1")
                )

                deps.add(
                    testImpl,
                    "org.junit.jupiter:junit-jupiter"
                )

                deps.add(
                    testImpl,
                    "org.junit.jupiter:junit-jupiter-api"
                )

                deps.add(testImpl, "org.postgresql:postgresql:42.7.1")
                deps.add(testImpl, "org.hamcrest:hamcrest-library:2.2")
                deps.add(testImpl, "org.xerial:sqlite-jdbc:3.45.0.0")
                deps.add(
                    testImpl,
                    deps.platform("org.testcontainers:testcontainers-bom:1.19.3")
                )
                deps.add(testImpl, "org.testcontainers:junit-jupiter")
                deps.add(testImpl, "org.testcontainers:postgresql")
                deps.add(testImpl, "org.testcontainers:mockserver")
                deps.add(testImpl, "org.testcontainers:kafka")

                deps.add(testImpl, "org.awaitility:awaitility:4.2.0")
                deps.add(testImpl, "org.assertj:assertj-core:3.25.1")
                deps.add(testImpl, "org.mockito:mockito-core:4.11.0")
                deps.add(testImpl, "org.mockito:mockito-inline:4.11.0")
                deps.add(testImpl, "org.mockito:mockito-junit-jupiter:4.11.0")

                deps.add(testImpl, "org.mock-server:mockserver-netty:5.14.0:shaded") {
                    exclude(group = "com.google.guava")
                    exclude(group = "com.fasterxml.jackson.core")
                    exclude(group = "com.fasterxml.jackson.datatype")
                    exclude(group = "com.fasterxml.jackson.dataformat")
                    exclude(group = "org.mock-server.mockserver-client-java")
                }
            }
        }
    }

    private fun configureTasks() {
        val tasks = p.tasks
        val sourceSetContainer = p.extensions.getByType(SourceSetContainer::class.java)

        val testVersionsTasks by tasks.registering(Task::class)

        val createAllDockerFilesTask = tasks.register("createAllDockerFiles") {
            group = "docker"
            description = "Create all docker files"
        }

        val buildAllDockerImages = tasks.register("buildAllDockerImages") {
            group = "docker"
            description = "Build all docker images"
        }

        val allJarsTask = tasks.register("jarAll") {
            group = "build"
            description =
                "Collection task, that is used to provide a single task that depends on the JAR tasks"
        }

        ext.sparkBuilds.all {
            val sparkFmt = sparkVersion.replace(".", "")

            scalaBinaryVersions.forEach { scalaBinaryVersion ->
                val scalaFmt = scalaBinaryVersion.replace(".", "")
                val sourceSetName = "spark${sparkFmt}Scala${scalaFmt}"
                val sourceSetTitle = "Spark${sparkFmt}Scala${scalaFmt}"
                val testSourceSetName = "test${sourceSetTitle}"
                val testSourceSetTitle = "Test${sourceSetTitle}"
                val testCompileTaskName = "compile${testSourceSetTitle}Java"

                val task = tasks.register<Test>("test${sourceSetTitle}") {
                    dependsOn(testCompileTaskName)
                    useJUnitPlatform {
                        excludeTags(
                            "integration-test",
                            "databricks",
                            "nonParallelTests",
                            "beforeShadowJarTest"
                        )
                        if (deltaVersion == null) {
                            excludeTags("delta")
                        }

                        if (icebergVersion == null) {
                            excludeTags("iceberg")
                        }
                    }
                    group = "verification"
                    classpath = sourceSetContainer[testSourceSetName].runtimeClasspath
                    testClassesDirs = sourceSetContainer[testSourceSetName].output.classesDirs
                }

                testVersionsTasks.configure {
                    dependsOn(task)
                }

                val downloadApacheSparkBinariesTask =
                    tasks.register<Download>("downloadApacheSparkBinaries${sourceSetTitle}") {
                        group = "docker"
                        description = "Download Apache Spark binaries for $sourceSetTitle"
                        src(determineApacheSparkBinaryUrl(sparkVersion, scalaBinaryVersion))
                        dest(p.layout.buildDirectory.file("docker/spark-${sparkVersion}/scala-${scalaBinaryVersion}/spark.tgz"))
                        overwrite(false)
                    }

                val downloadApacheSparkBinariesAscTask =
                    tasks.register<Download>("downloadApacheSparkBinariesAsc${sourceSetTitle}") {
                        group = "docker"
                        description = "Download Apache Spark binaries asc for $sourceSetTitle"
                        src(determineApacheSparkBinaryAscUrl(sparkVersion, scalaBinaryVersion))
                        dest(p.layout.buildDirectory.file("docker/spark-${sparkVersion}/scala-${scalaBinaryVersion}/spark.tgz.asc"))
                        overwrite(false)
                    }

                val downloadKeys = tasks.register<Download>("downloadKeys${sourceSetTitle}") {
                    group = "docker"
                    description = "Download PGP keys for $sourceSetTitle"
                    src("https://downloads.apache.org/spark/KEYS")
                    dest(p.layout.buildDirectory.file("docker/spark-${sparkVersion}/scala-${scalaBinaryVersion}/KEYS"))
                    overwrite(false)
                }

                val copyEntryPoint = tasks.register<Copy>("copyEntryPoint$sourceSetTitle") {
                    group = "docker"
                    description = "Copy entrypoint.sh for $sourceSetTitle"
                    from(p.file("entrypoint.sh"))
                    into(p.layout.buildDirectory.dir("docker/spark-${sparkVersion}/scala-${scalaBinaryVersion}"))
                }

                val createDockerFile = tasks.register("createDockerFile${sourceSetTitle}") {
                    group = "docker"
                    description = "Create docker file for $sourceSetTitle"

                    val destinationFile = p.file("build/docker/spark-${sparkVersion}/scala-${scalaBinaryVersion}/Dockerfile")
                    outputs.file(destinationFile)
                    doLast {
                        destinationFile.writeText(
                            """FROM azul/zulu-openjdk:8-latest

ARG spark_uid=5555

RUN set -ex; \
    groupadd --system --gid=${'$'}{spark_uid} spark && useradd --system --uid=${'$'}{spark_uid} --gid=spark spark

RUN set -ex; \
    apt-get update; \
    apt-get install -y gnupg2 wget bash tini libc6 libpam-modules krb5-user libnss3 procps net-tools gosu libnss-wrapper python3; \
    mkdir -p /opt/spark; \
    mkdir /opt/spark/python; \
    mkdir -p /opt/spark/examples; \
    mkdir -p /opt/spark/work-dir; \
    chmod g+w /opt/spark/work-dir; \
    touch /opt/spark/RELEASE; \
    chown -R spark:spark /opt/spark; \
    echo "auth required pam_wheel.so use_uid" >> /etc/pam.d/su; \
    rm -rf /var/lib/apt/lists/*;

ENV SPARK_TMP=/tmp/spark
WORKDIR ${'$'}SPARK_TMP

COPY spark.tgz .
COPY spark.tgz.asc .
COPY KEYS .

RUN set -ex; \
    export GNUPGHOME="${'$'}(mktemp -d)"; \
    gpg --batch --import KEYS; \
    gpg --batch --verify spark.tgz.asc spark.tgz; \
    gpgconf --kill all; \
    rm -rf "${'$'}GNUPGHOME" spark.tgz.asc; \
    \
    tar -xf spark.tgz --strip-components=1; \
    chown -R spark:spark .; \
    mv bin /opt/spark/; \
    mv conf /opt/spark/; \
    mv data /opt/spark/; \
    mv examples /opt/spark/; \
    mv jars /opt/spark/; \
    mv python/lib /opt/spark/python/lib/; \
    mv python/pyspark /opt/spark/python/pyspark/; \
    mv R /opt/spark/; \
    mv sbin /opt/spark/; \
    cd ..; \
    rm -rf "${'$'}SPARK_TMP";

COPY entrypoint.sh /opt/entrypoint.sh

RUN chmod +x /opt/entrypoint.sh

ENV SPARK_HOME=/opt/spark
ENV PATH=${'$'}PATH:${'$'}SPARK_HOME/bin

WORKDIR /opt/spark/work-dir

USER spark

ENTRYPOINT [ "/opt/entrypoint.sh" ]
""".trimIndent()
                        )
                    }
                }

                createAllDockerFilesTask.configure {
                    dependsOn(createDockerFile)
                }

                val buildDockerImage = tasks.register("buildDockerImage${sourceSetTitle}") {
                    dependsOn(
                        createDockerFile,
                        copyEntryPoint,
                        downloadApacheSparkBinariesTask,
                        downloadApacheSparkBinariesAscTask,
                        downloadKeys
                    )
                    group = "docker"
                    description = "Build docker image for $sourceSetTitle"

                    doLast {
                        p.exec {
                            commandLine(
                                "docker",
                                "build",
                                "-t",
                                "openlineage/spark:spark-${sparkVersion}-scala-${scalaBinaryVersion}",
                                "."
                            )
                            workingDir = p.file("build/docker/spark-${sparkVersion}/scala-${scalaBinaryVersion}")
                        }
                    }
                }

                buildAllDockerImages.configure {
                    dependsOn(buildDockerImage)
                }

                val jarTask = tasks.register<Jar>("jar${sourceSetTitle}") {
                    group = "build"
                    description =
                        "Assembles a jar archive containing the main classes for $sourceSetTitle"
                    archiveBaseName.set("openlineage-spark-app_${scalaBinaryVersion}")
                    from(sourceSetContainer[sourceSetName].output)
                    destinationDirectory.set(p.file("build/libs/${sourceSetName}"))
                }

                allJarsTask.configure {
                    dependsOn(jarTask)
                }

                val creationVersionProperties = tasks.register("createVersionProperties${sourceSetTitle}") {
                    dependsOn(tasks.named("process${sourceSetTitle}Resources"))
                    doLast {
                        val dir = p.layout.buildDirectory.dir("resources/${sourceSetName}/io/openlineage/spark/agent/").get().asFile
                        dir.mkdirs()
                        p.layout.buildDirectory.file("resources/${sourceSetName}/io/openlineage/spark/agent/version.properties")
                            .get()
                            .asFile
                            .writer().use { writer ->
                            val properties = Properties()
                            properties["version"] = project.version.toString()
                            properties.store(writer, null)
                        }
                    }
                }

                val copyDependencies = tasks.register<Copy>("copyDependencies${sourceSetTitle}") {
                    dependsOn(jarTask)
                    group = "build"
                    description = "Copy dependencies for $sourceSetTitle"
                    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
                    from(p.configurations["${sourceSetName}RuntimeClasspath"])
                    into(p.layout.buildDirectory.dir("dependencies/${sourceSetName}").get())
                    include("*.jar")
                }

                val shadowJarTask = tasks.register<ShadowJar>("shadowJar${sourceSetTitle}") {
                    dependsOn(jarTask, creationVersionProperties)

                    group = "build"
                    description = "Assembles a shaded jar for $sourceSetTitle"
                    archiveBaseName.set("openlineage-spark-app_${scalaBinaryVersion}")
                    from(sourceSetContainer[sourceSetName].output)
                    configurations = listOf(p.configurations["${sourceSetName}RuntimeClasspath"])
                    destinationDirectory.set(p.file("build/libs/${sourceSetName}"))

                    minimize {
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":shared",
                                    "configuration" to "scala${scalaFmt}RuntimeElements"
                                )
                            )
                        )
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":spark2",
                                    "configuration" to "scala212RuntimeElements"
                                )
                            )
                        )
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":spark3",
                                    "configuration" to "scala${scalaFmt}RuntimeElements"
                                )
                            )
                        )
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":spark31",
                                    "configuration" to "scala212RuntimeElements"
                                )
                            )
                        )
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":spark32",
                                    "configuration" to "scala${scalaFmt}RuntimeElements"
                                )
                            )
                        )
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":spark33",
                                    "configuration" to "scala${scalaFmt}RuntimeElements"
                                )
                            )
                        )
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":spark34",
                                    "configuration" to "scala${scalaFmt}RuntimeElements"
                                )
                            )
                        )
                        exclude(
                            project(
                                mapOf(
                                    "path" to ":spark35",
                                    "configuration" to "scala${scalaFmt}RuntimeElements"
                                )
                            )
                        )
                    }

                    archiveClassifier = ""
                    relocate(
                        "com.github.ok2c.hc5",
                        "io.openlineage.spark.shaded.com.github.ok2c.hc5"
                    )
                    relocate(
                        "org.apache.httpcomponents.client5",
                        "io.openlineage.spark.shaded.org.apache.httpcomponents.client5"
                    )
                    relocate("javassist", "io.openlineage.spark.shaded.javassist")
                    relocate("org.apache.hc", "io.openlineage.spark.shaded.org.apache.hc")
                    relocate(
                        "org.apache.commons.codec",
                        "io.openlineage.spark.shaded.org.apache.commons.codec"
                    )
                    relocate(
                        "org.apache.commons.lang3",
                        "io.openlineage.spark.shaded.org.apache.commons.lang3"
                    )
                    relocate(
                        "org.apache.commons.beanutils",
                        "io.openlineage.spark.shaded.org.apache.commons.beanutils"
                    )
                    relocate("org.apache.http", "io.openlineage.spark.shaded.org.apache.http")
                    relocate("org.yaml.snakeyaml", "io.openlineage.spark.shaded.org.yaml.snakeyaml")
                    relocate("org.slf4j", "io.openlineage.spark.shaded.org.slf4j")
                    relocate(
                        "com.fasterxml.jackson",
                        "io.openlineage.spark.shaded.com.fasterxml.jackson"
                    ) {
                        exclude("com.fasterxml.jackson.annotation.JsonIgnore")
                        exclude("com.fasterxml.jackson.annotation.JsonIgnoreProperties")
                        exclude("com.fasterxml.jackson.annotation.JsonIgnoreType")
                    }
                    manifest {
                        attributes(
                            mapOf(
                                "Created-By" to "Gradle ${project.gradle.gradleVersion}",
                                "Built-By" to System.getProperty("user.name"),
                                "Build-Jdk" to System.getProperty("java.version"),
                                "Implementation-Title" to project.name,
                                "Implementation-Version" to project.version
                            )
                        )
                    }
                    isZip64 = true
                }

                tasks.register<Test>("integrationTest${sourceSetTitle}") {
                    dependsOn(shadowJarTask, copyDependencies)
                    useJUnitPlatform {
                        includeTags("integration-test")
                        excludeTags("delta", "iceberg", "beforeShadowJarTest", "databricks")
                    }

                    testLogging {
                        events("passed", "skipped", "failed")
                        showStandardStreams = true
                    }

                    systemProperties["build.dependencies"] = p.file("build/dependencies/${sourceSetName}")
                    systemProperties["docker.image.name"] =
                        "openlineage/spark:spark-${sparkVersion}-scala-${scalaBinaryVersion}"
                    systemProperties["spark.version"] = sparkVersion
                    systemProperties["scala.binary.version"] = scalaBinaryVersion
                    systemProperties["openlineage.lib.dir"] = p.file("build/libs/${sourceSetName}")
                    systemProperties["openlineage.spark.jar"] =
                        "openlineage-spark-app_${scalaBinaryVersion}-${p.version}.jar"
                    systemProperties["test.results.dir"] =
                        p.file("build/test-results/integrationTest${sourceSetTitle}")

                    group = "verification"
                    classpath = sourceSetContainer[testSourceSetName].runtimeClasspath
                    testClassesDirs = sourceSetContainer[testSourceSetName].output.classesDirs

                }
            }
        }
    }

    companion object {
        private val ARCHIVE_URL_LOOK_UP = mapOf(
            "2.4.8" to mapOf(
                "2.11" to Pair(
                    "spark-2.4.8/spark-2.4.8-bin-without-hadoop.tgz",
                    "spark-2.4.8/spark-2.4.8-bin-without-hadoop.tgz.asc"
                ),
                "2.12" to Pair(
                    "spark-2.4.8/spark-2.4.8-bin-without-hadoop-scala-2.12.tgz",
                    "spark-2.4.8/spark-2.4.8-bin-without-hadoop-scala-2.12.tgz.asc"
                )
            ),
            "3.2.4" to mapOf(
                "2.12" to Pair(
                    "spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz",
                    "spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz.asc"
                ),
                "2.13" to Pair(
                    "spark-3.2.4/spark-3.2.4-bin-hadoop3.2-scala2.13.tgz",
                    "spark-3.2.4/spark-3.2.4-bin-hadoop3.2-scala2.13.tgz.asc"
                )
            ),
            "3.3.4" to mapOf(
                "2.12" to Pair(
                    "spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz",
                    "spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz.asc"
                ),
                "2.13" to Pair(
                    "spark-3.3.4/spark-3.3.4-bin-hadoop3-scala2.13.tgz",
                    "spark-3.3.4/spark-3.3.4-bin-hadoop3-scala2.13.tgz.asc"
                )
            ),
            "3.4.2" to mapOf(
                "2.12" to Pair(
                    "spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz",
                    "spark-3.4.2/spark-3.4.2-bin-hadoop3.tgz.asc"
                ),
                "2.13" to Pair(
                    "spark-3.4.2/spark-3.4.2-bin-hadoop3-scala2.13.tgz",
                    "spark-3.4.2/spark-3.4.2-bin-hadoop3-scala2.13.tgz.asc"
                )
            ),
            "3.5.0" to mapOf(
                "2.12" to Pair(
                    "spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz",
                    "spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz.asc"
                ),
                "2.13" to Pair(
                    "spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz",
                    "spark-3.5.0/spark-3.5.0-bin-hadoop3-scala2.13.tgz.asc"
                )
            )
        )

        private fun determineApacheSparkBinaryUrl(
            sparkVersion: String,
            scalaBinaryVersion: String
        ): String {
            return "https://dlcdn.apache.org/spark/${ARCHIVE_URL_LOOK_UP[sparkVersion]!![scalaBinaryVersion]!!.first}"
        }

        private fun determineApacheSparkBinaryAscUrl(
            sparkVersion: String,
            scalaBinaryVersion: String
        ): String {
            return "https://dlcdn.apache.org/spark/${ARCHIVE_URL_LOOK_UP[sparkVersion]!![scalaBinaryVersion]!!.second}"
        }
    }
}

fun DependencyHandler.addMulti(
    configurations: List<String>,
    dependencyNotation: String,
    configure: ExternalModuleDependency.() -> Unit = {}
) {
    configurations.forEach { configurationName ->
        add(configurationName, dependencyNotation, configure)
    }
}

fun DependencyHandler.addMulti(
    configurations: List<String>,
    group: String,
    name: String,
    version: String?,
    configure: ExternalModuleDependency.() -> Unit = {}
) {
    configurations.forEach { configurationName ->
        addIfPresent(configurationName, group, name, version, configure)
    }
}

fun DependencyHandler.addIfPresent(
    configurationName: String,
    group: String,
    name: String,
    version: String?,
    configure: ExternalModuleDependency.() -> Unit = {}
) {
    if (version != null) {
        add(configurationName, "$group:$name:$version", configure)
    }
}


open class MultiSparkBuildPluginExtension @Inject constructor(objectFactory: ObjectFactory) {
    val sparkBuilds: DomainObjectCollection<ModuleConfig> =
        objectFactory.domainObjectContainer(ModuleConfig::class.java)

    fun add(
        name: String,
        sparkVersion: String,
        scalaBinaryVersions: List<String>,
        icebergVersion: String?,
        deltaVersion: String?,
        snowflakeVersion: String?,
        googleCloudVersion: String?,
        hadoopClientVersion: String?
    ) {
        sparkBuilds.add(
            ModuleConfig(
                name,
                sparkVersion,
                scalaBinaryVersions,
                icebergVersion,
                deltaVersion,
                snowflakeVersion,
                googleCloudVersion,
                hadoopClientVersion
            )
        )
    }
}

data class ModuleConfig(
    val name: String,
    val sparkVersion: String,
    val scalaBinaryVersions: List<String>,
    val icebergVersion: String?,
    val deltaVersion: String?,
    val snowflakeVersion: String?,
    val bigqueryVersion: String?,
    val hadoopClientVersion: String?
)
