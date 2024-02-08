/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import com.github.jengelman.gradle.plugins.shadow.ShadowPlugin
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar
import org.gradle.api.Project
import org.gradle.api.Task
import org.gradle.api.tasks.Copy
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.TaskProvider
import org.gradle.api.tasks.compile.JavaCompile
import org.gradle.api.tasks.testing.Test
import org.gradle.jvm.tasks.Jar
import org.gradle.kotlin.dsl.assign
import org.gradle.kotlin.dsl.get
import org.gradle.kotlin.dsl.register
import org.gradle.kotlin.dsl.withType
import java.util.*

/**
 * Registers all the necessary tasks for a given build variant within the project. This includes tasks for
 * unit testing, JAR packaging, cleanup, integration tests, and others specifically configured for the [InternalSparkVariantBuild].
 *
 * @receiver The Gradle [Project] within which the tasks are being registered.
 * @param b The [InternalSparkVariantBuild] instance representing the build variant for which the tasks are being defined.
 */
@Suppress("UNUSED_VARIABLE")
internal fun Project.registerTasks(b: InternalSparkVariantBuild) {
    val createVersionPropertiesTask = registerCreateVersionPropertiesTask(b)
    val jarTask = registerJarTask(b)
    val deleteSparkDirs = registerCleanupTask(b)
    val unitTestTask = registerUnitTestTask(b, deleteSparkDirs)
    val copyAdditionalJarsTask = registerCopyAdditionalIntegrationTestJars(b)
    val copyFixturesTask = registerCopyIntegrationTestFixtures(b)
    val integrationTestTask =
        registerIntegrationTestTask(b, copyAdditionalJarsTask, copyFixturesTask)
    val shadowJarTask = registerShadowJarTask(b, jarTask)
}

/**
 * Registers a task for running unit tests specific to a [InternalSparkVariantBuild].
 * This task relies on the standard JUnit platform setup to execute tests,
 * excluding any tagged as 'integration-test'. It ensures that system properties
 * defined by the build variant are propagated to the test execution environment.
 *
 * @param b The build variant configuration representing the specific Spark and Scala version.
 * @param deleteSparkDirs A task provider for a task that cleans up Spark-related directories before executing tests.
 * @return The task provider for the registered unit test task.
 */
internal fun Project.registerUnitTestTask(
    b: InternalSparkVariantBuild,
    deleteSparkDirs: TaskProvider<Delete>
): TaskProvider<Test> {
    val unitTestTaskName = "executeUnitTestsFor${b.name}"
    val unitTestTask = tasks.register<Test>(unitTestTaskName)
    unitTestTask.configure {
        dependsOn(deleteSparkDirs)
        group = "unitTests"
        description = "Runs unit tests for Spark variant: ${b.name}"
        testClassesDirs = b.testSourceSet.output.classesDirs
        classpath = b.testSourceSet.runtimeClasspath
        useJUnitPlatform {
            excludeTags("integration-test")
        }

        doFirst {
            b.updateSystemProperties(systemProperties)
        }
    }
    return unitTestTask
}

/**
 * Registers a cleanup task to delete Spark framework-related directories for a given [InternalSparkVariantBuild].
 * This is useful for ensuring a clean state before or after test runs,
 * particularly for integration tests that may modify these directories.
 *
 * @param b The build variant configuration.
 * @return The task provider for the registered cleanup task.
 */
internal fun Project.registerCleanupTask(b: InternalSparkVariantBuild): TaskProvider<Delete> =
    tasks.register<Delete>("cleanUpSparkDirsFor${b.name}") {
        group = "aux"
        description = "Cleans up the Spark directories for variant: ${b.name}"
        delete(b.prop(InternalSparkVariantBuild.DERBY_SYSTEM_HOME))
        delete(b.prop(InternalSparkVariantBuild.SPARK_WAREHOUSE))
    }

/**
 * Registers a task to create a 'version.properties' file within the resources directory of a [InternalSparkVariantBuild].
 * This properties file includes project version information that can be accessed at runtime,
 * useful for logging or display purposes within the application.
 *
 * @param b The build variant configuration for which the properties file is created.
 * @return The task provider for the registered properties file creation task.
 */
internal fun Project.registerCreateVersionPropertiesTask(b: InternalSparkVariantBuild): TaskProvider<Task> {
    val task = tasks.register("createVersionPropertiesFor${b.name}") {
        dependsOn(b.mainSourceSet.processResourcesTaskName)
        group = "resources"
        description = "Creates a version.properties file for Spark variant: ${b.name}"
        outputs.file(layout.buildDirectory.file("resources/${b.mainSourceSet.name}/io/openlineage/spark/agent/version.properties"))
        doLast {
            val propertiesFile = outputs.files.first()
            propertiesFile.writer().use { writer ->
                val properties = Properties()
                properties["version"] = project.version.toString()
                properties.store(writer, null)
            }
        }
    }
    tasks.named(b.mainSourceSet.classesTaskName).configure {
        dependsOn(task)
    }
    return task
}

/**
 * Registers a JAR packaging task for the main source set of a [InternalSparkVariantBuild].
 * This task assembles a JAR file containing the compiled classes and resources
 * of the main source set, ready for distribution or further testing.
 *
 * @param b The build variant configuration being packaged into a JAR.
 * @return The task provider for the registered JAR task.
 */
internal fun Project.registerJarTask(b: InternalSparkVariantBuild) =
    tasks.register<Jar>(b.mainSourceSet.jarTaskName) {
        dependsOn(b.mainSourceSet.classesTaskName)
        group = "build"
        description = "Assembles a jar archive for Spark variant: ${b.name}"
        archiveBaseName = "openlineage-spark"
        archiveAppendix = "app_${b.scalaBinaryVersion}"
        from(b.mainSourceSet.output)
        destinationDirectory.set(layout.buildDirectory.dir("libs/spark-${b.sparkVersion}/scala-${b.scalaBinaryVersion}"))
    }

/**
 * Registers a shadow JAR task for a [InternalSparkVariantBuild], creating a fat JAR that includes
 * all dependencies along with the build variant's compiled code. This is particularly useful for
 * distribution or running standalone applications that require a complete package of all dependencies.
 *
 * @param b The build variant configuration being packaged into a shadow JAR.
 * @param jarTask The task provider for the standard JAR task, which the shadow JAR task depends on.
 * @return The task provider for the registered shadow JAR task.
 */
internal fun Project.registerShadowJarTask(
    b: InternalSparkVariantBuild,
    jarTask: TaskProvider<Jar>
): TaskProvider<ShadowJar> {
    return tasks.register<ShadowJar>(b.shadowJarTaskName) {
        dependsOn(jarTask)
        group = "shadow"
        description = "Assembles a shadow jar archive for Spark variant: ${b.name}"
        archiveBaseName = "openlineage-spark"
        archiveAppendix = "app_${b.scalaBinaryVersion}"
        archiveClassifier = "shadow"
        from(b.mainSourceSet.output)
        configurations = listOf(project.configurations[b.mainRuntimeClassPath])
        isZip64 = true
        destinationDirectory = jarTask.flatMap { it.destinationDirectory }

        // This should be exposed in the build script. It's too deep here.
        val prefix = "io.openlineage.spark.shaded"
        relocate(
            "com.github.ok2c.hc5",
            "${prefix}.com.github.ok2c.hc5"
        )
        relocate(
            "org.apache.httpcomponents.client5",
            "${prefix}.org.apache.httpcomponents.client5"
        )
        relocate("javassist", "${prefix}.javassist")
        relocate("org.apache.hc", "${prefix}.org.apache.hc")
        relocate(
            "org.apache.commons.codec",
            "${prefix}.org.apache.commons.codec"
        )
        relocate(
            "org.apache.commons.lang3",
            "${prefix}.org.apache.commons.lang3"
        )
        relocate(
            "org.apache.commons.beanutils",
            "${prefix}.org.apache.commons.beanutils"
        )
        relocate("org.apache.http", "${prefix}.org.apache.http")
        relocate("org.yaml.snakeyaml", "${prefix}.org.yaml.snakeyaml")
        relocate(
            "com.fasterxml.jackson",
            "${prefix}.com.fasterxml.jackson"
        ) {
            exclude("com.fasterxml.jackson.annotation.JsonIgnore")
            exclude("com.fasterxml.jackson.annotation.JsonIgnoreProperties")
            exclude("com.fasterxml.jackson.annotation.JsonIgnoreType")
        }

        dependencies {
            exclude(dependency("org.slf4j::"))
            exclude("org/apache/commons/logging/**")
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
    }
}

/**
 * Registers a task to copy additional JARs required for integration testing of a [InternalSparkVariantBuild].
 * These include custom or third-party JARs not included in the project's declared dependencies
 * but are necessary for integration tests to execute properly.
 *
 * @param b The build variant configuration.
 * @return The task provider for the registered copy task.
 */
internal fun Project.registerCopyAdditionalIntegrationTestJars(b: InternalSparkVariantBuild): TaskProvider<Copy> {
    val copyDependenciesTask = tasks.register<Copy>(b.integrationTestCopyAdditionalJarsTaskName) {
        group = "integrationTests"
        description =
            "Copies additional jars needed for the integration tests for Spark variant: ${b.name}"
        from(configurations[b.integrationTestAdditionalJars])
        into(b.prop(InternalSparkVariantBuild.ADDITIONAL_JARS_DIR)!!)
        include("*.jar")
    }
    return copyDependenciesTask
}

/**
 * Registers a task to copy test fixtures necessary for integration testing of a [InternalSparkVariantBuild].
 * These fixtures might include data files, configuration files, or other resources that are required
 * for the setup or execution of integration tests.
 *
 * @param b The build variant configuration.
 * @return The task provider for the registered copy task.
 */
internal fun Project.registerCopyIntegrationTestFixtures(b: InternalSparkVariantBuild): TaskProvider<Copy> {
    val copyFixturesTask = tasks.register<Copy>(b.integrationTestCopyTestFixturesTaskName) {
        group = "integrationTests"
        description =
            "Copies test fixtures needed for the integration tests for Spark variant: ${b.name}"
        from(configurations[b.integrationTestAdditionalJars])
        into(b.prop(InternalSparkVariantBuild.FIXTURES_DIR)!!)
    }
    return copyFixturesTask
}

/**
 * Registers an integration test task for a [InternalSparkVariantBuild], configuring it to run
 * tests tagged with 'integration-test'. This task setup includes the handling of system properties
 * for test execution context and configuring the test runtime classpath to include necessary dependencies.
 *
 * @param b The build variant configuration.
 * @param copyAdditionalJarsTask A task provider for copying additional JARs needed for integration testing.
 * @param copyFixturesTask A task provider for copying test fixtures necessary for integration testing.
 * @return The task provider for the registered integration test task.
 */
internal fun Project.registerIntegrationTestTask(
    b: InternalSparkVariantBuild,
    copyAdditionalJarsTask: TaskProvider<Copy>,
    copyFixturesTask: TaskProvider<Copy>
): TaskProvider<Test> {
    val integrationTestTask = tasks.register<Test>(b.integrationTestTaskName)
    integrationTestTask.configure {
        dependsOn(b.shadowJarTaskName, copyAdditionalJarsTask, copyFixturesTask)
        group = "integrationTests"
        description = "Runs integration tests for Spark variant: ${b.name}"
        testClassesDirs = b.testSourceSet.output.classesDirs
        classpath = b.testSourceSet.runtimeClasspath
        useJUnitPlatform {
            includeTags("integration-test")
            excludeTags("databricks")
        }

        doFirst {
            b.updateSystemProperties(systemProperties)
            systemProperties["project.version"] = project.version.toString()

            tasks.getByName(b.shadowJarTaskName).outputs.files.first().let {
                systemProperties["openlineage.spark.agent.jar"] = it.name
                systemProperties["openlineage.spark.agent.jar.dir"] = it.parent
                systemProperties["openlineage.spark.agent.jar.path"] = it.absolutePath
            }
            val additionalJarsDir =
                copyAdditionalJarsTask.map { it.destinationDir }.get().absolutePath
            systemProperties["openlineage.spark.agent.additional.jars.dir"] = additionalJarsDir
            val fixturesDir = copyFixturesTask.map { it.destinationDir }.get().absolutePath
            systemProperties["openlineage.spark.agent.fixtures.dir"] = fixturesDir
        }
    }
    return integrationTestTask
}

/**
 * Registers a task to print the details of all source sets in the project, providing insight into the Java and
 * resource directories, as well as the compile and runtime classpaths for each source set. This function is
 * beneficial for debugging and verifying the configuration of source sets.
 *
 * @receiver The Gradle [Project] within which the task is being registered.
 */
internal fun Project.registerPrintSourceSetDetailsTasks() {
    val sourceSetContainer = extensions.getByType(SourceSetContainer::class.java)
    sourceSetContainer.configureEach {
        val taskName = "printSourceSetDetails${name.capitalize()}"
        tasks.register(taskName) {
            group = "debug"
            description = "Prints details for source set: $name"
            doLast {
                println("Source set: $name\n")
                println("> Java source dirs: ${java.srcDirs.joinToString(", ")}\n")
                println("> Resources source dirs: ${resources.srcDirs.joinToString(", ")}\n")
                println("> Output dirs: ${output.classesDirs.joinToString(", ")}\n")
                val sortedCompileClassPath =
                    compileClasspath.sortedBy { it.absolutePath }.joinToString("\n - ", " - ")

                println("> Compile classpath:\n${sortedCompileClassPath}\n")

                val sortedRuntimeClasspath =
                    runtimeClasspath.sortedBy { it.absolutePath }.joinToString("\n - ", " - ")
                println("> Runtime classpath:\n${sortedRuntimeClasspath}\n")
            }
        }
    }
}

/**
 * Registers an aggregate task to run unit tests for all Spark variants in the project. This task aggregates
 * unit test tasks across all Spark variants, enabling a single command execution to run tests against all configurations.
 *
 * @receiver The Gradle [Project] in which the aggregate unit test task is being registered.
 */
internal fun Project.registerAggregateUnitTestTask() {
    tasks.register("executeAllVariantsUnitTests") {
        group = "aggregate"
        description = "Aggregate task to run all Spark variant tests"
        dependsOn(
            tasks.withType<Test>().matching { it.group == "unitTests" && it.name != this.name })
    }
}

/**
 * Registers an aggregate task to assemble JARs for all Spark variants in the project. It aggregates the JAR tasks
 * of all defined Spark variants into a single task that can be executed to package all variants at once.
 *
 * @receiver The Gradle [Project] in which the aggregate JAR task is being registered.
 */
internal fun Project.registerAggregateJarTask() {
    tasks.register("executeAllVariantsJarTasks") {
        group = "aggregate"
        description = "Aggregate task to assemble all Spark variant jar archives"
        dependsOn(
            tasks.withType<Jar>().matching { it.group == "build" && it.name.endsWith("Jar") })
    }
}

/**
 * Registers an aggregate task for creating shadow JARs for all Spark variants that are configured in the project.
 * This facilitates the creation of fat JARs, containing all dependencies, for each Spark and Scala version variant.
 *
 * @receiver The Gradle [Project] where the aggregate shadow JAR task is being registered.
 */
internal fun Project.registerAggregateShadowJarTasks() {
    plugins.withType<ShadowPlugin> {
        tasks.register("executeAllVariantsShadowJar") {
            group = "aggregate"
            description = "Aggregate task to create shadow jars for all Spark variants"
            dependsOn(
                tasks.withType<ShadowJar>()
                    .filter { it.name.startsWith("mainSpark") && it.name.endsWith("ShadowJar") })
        }
    }
}

/**
 * Registers an aggregate task to compile Java sources for all Spark variants in the project. It aggregates
 * the compilation tasks of all defined Spark variants into a single task that can be executed to compile
 * all variants at once.
 *
 * @receiver The Gradle [Project] in which the aggregate compile task is being registered.
 */
internal fun Project.registerAggregateCompileMainSourcesTask() {
    tasks.register("compileAllVariantsJava") {
        group = "aggregate"
        description = "Aggregate task to compile all Spark variants"
        dependsOn(
            tasks.withType<JavaCompile>()
                .filter { it.name.startsWith("compileSpark") })
    }
}

/**
 * Registers an aggregate task to compile Java test sources for all Spark variants in the project. It aggregates
 * the test compilation tasks of all defined Spark variants into a single task that can be executed to compile
 * test sources for all variants at once.
 *
 * @receiver The Gradle [Project] in which the aggregate test compile task is being registered.
 */
internal fun Project.registerAggregateCompileTestSourcesTask() {
    tasks.register("compileAllVariantsTestJava") {
        group = "aggregate"
        description = "Aggregate task to compile all Spark test variants"
        dependsOn(
            tasks.withType<JavaCompile>()
                .filter { it.name.startsWith("compileTestSpark") })
    }
}

/**
 * Registers an aggregate task to run integration tests for all Spark variants in the project. This task
 * aggregates integration test tasks across all Spark variants, allowing execution of all integration tests with
 * a single command.
 *
 * @receiver The Gradle [Project] in which the aggregate integration test task is being registered.
 */
internal fun Project.registerAggregateIntegrationTestTask() {
    tasks.register("executeAllVariantsIntegrationTests") {
        group = "aggregate"
        description = "Aggregate task to run all Spark variant integration tests"
        dependsOn(
            tasks.withType<Test>().matching { it.group == "integrationTests" })
    }
}
