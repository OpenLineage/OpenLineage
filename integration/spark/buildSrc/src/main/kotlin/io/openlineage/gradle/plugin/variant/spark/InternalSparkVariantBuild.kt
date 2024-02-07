/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet

abstract class InternalSparkVariantBuild @javax.inject.Inject constructor(
    val name: String,
    val sparkVersion: String,
    val scalaBinaryVersion: String,
    internal val project: Project,
    val mainSourceSet: SourceSet,
    val testSourceSet: SourceSet
) {
    companion object {
        const val DOCKER_IMAGE_NAME = "docker.image.name"
        const val DELTA_TESTS_ENABLED = "delta.tests.enabled"
        const val ICEBERG_TESTS_ENABLED = "iceberg.tests.enabled"
        const val DERBY_SYSTEM_HOME = "derby.system.home"
        const val SPARK_WAREHOUSE = "spark.warehouse.dir"
        const val JUNIT_CAPTURE_STD_OUT = "junit.platform.output.capture.stdout"
        const val JUNIT_CAPTURE_STD_ERR = "junit.platform.output.capture.stderr"
        const val SPARK_VERSION = "spark.version"
        const val SCALA_BINARY_VERSION = "scala.binary.version"

        /**
         * This is used to specify the Kafka package in GAV (Group:Artifact:Version) format.
         * The value of this will be supplied to the `--packages` argument of the `spark-submit` command.
         *
         * For example:
         *
         * ```shell
         * spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.2 ...
         * ```
         */
        const val KAFKA_PACKAGE_VERSION = "kafka.package.version"

        const val ADDITIONAL_JARS_DIR = "additional.jars.dir"

        const val FIXTURES_DIR = "fixtures.dir"
    }

    internal val dependencies = project.dependencies
    private val properties = sortedMapOf<String, Any>()

    init {
        properties[DELTA_TESTS_ENABLED] = true

        properties[ICEBERG_TESTS_ENABLED] = true

        properties[JUNIT_CAPTURE_STD_OUT] = true

        properties[JUNIT_CAPTURE_STD_ERR] = true

        properties[DERBY_SYSTEM_HOME] =
            project.layout.projectDirectory.file("build/var/run/${testSourceSet.name}/derby")

        properties[SPARK_WAREHOUSE] =
            project.layout.projectDirectory.dir("build/var/run/${testSourceSet.name}/spark-warehouse")

        properties[SPARK_VERSION] = sparkVersion

        properties[SCALA_BINARY_VERSION] = scalaBinaryVersion

        properties[KAFKA_PACKAGE_VERSION] =
            "org.apache.spark:spark-sql-kafka-0-10_${scalaBinaryVersion}:${sparkVersion}"

        properties[ADDITIONAL_JARS_DIR] =
            "build/deps/spark-${sparkVersion}/scala-${scalaBinaryVersion}"

        properties[FIXTURES_DIR] =
            "build/fixtures/spark-${sparkVersion}/scala-${scalaBinaryVersion}"

        properties[DOCKER_IMAGE_NAME] =
            "openlineage/spark:spark-${sparkVersion}-scala-${scalaBinaryVersion}"
    }

    val mainApi = mainSourceSet.apiConfigurationName
    val mainApiElements = mainSourceSet.apiElementsConfigurationName
    val mainCompileClassPath = mainSourceSet.compileClasspathConfigurationName
    val mainCompileOnly = mainSourceSet.compileOnlyConfigurationName
    val mainCompileOnlyApi = mainSourceSet.compileOnlyApiConfigurationName
    val mainImplementation = mainSourceSet.implementationConfigurationName
    val mainRuntimeClassPath = mainSourceSet.runtimeClasspathConfigurationName
    val mainRuntimeElements = mainSourceSet.runtimeElementsConfigurationName
    val mainRuntimeOnly = mainSourceSet.runtimeOnlyConfigurationName

    val testCompileClasspath = testSourceSet.compileClasspathConfigurationName
    val testCompileOnly = testSourceSet.compileOnlyConfigurationName
    val testImplementation = testSourceSet.implementationConfigurationName
    val testRuntimeClasspath = testSourceSet.runtimeClasspathConfigurationName
    val testRuntimeOnly = testSourceSet.runtimeOnlyConfigurationName

    val integrationTestRuntimeOnly = "integrationTest${mainSourceSet.name.capitalize()}RuntimeOnly"
    val integrationTestRuntimeClasspath =
        "integrationTest${mainSourceSet.name.capitalize()}RuntimeClasspath"

    /**
     * This is the name of a configuration that should be used for the explicit purpose of mounting JARs to a running Docker container.
     */
    val integrationTestAdditionalJars =
        "integrationTest${mainSourceSet.name.capitalize()}Dependencies"

    val integrationTestFixtures = "integrationTest${mainSourceSet.name.capitalize()}Fixtures"


    val integrationTestTaskName = "executeIntegrationTestsFor${mainSourceSet.name.capitalize()}"

    val integrationTestCopyAdditionalJarsTaskName =
        "copyIntegrationTest${mainSourceSet.name.capitalize()}AdditionalJars"

    val integrationTestCopyTestFixturesTaskName =
        "copyIntegrationTest${mainSourceSet.name.capitalize()}Fixtures"

    val shadowJarTaskName = "${mainSourceSet.name}ShadowJar"

    fun configure(configurer: BuildConfigurerDsl.() -> Unit) {
        val dsl = BuildConfigurerDsl(this)
        configurer.invoke(dsl)
    }

    fun prop(key: String, value: Any) {
        properties[key] = value
    }

    fun prop(key: String) = properties[key]

    fun updateSystemProperties(systemProperties: MutableMap<String, Any>) {
        properties.forEach { (k, v) -> systemProperties[k] = v.toString() }
    }
}
