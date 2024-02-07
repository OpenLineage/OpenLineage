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
            excludeTags("nonParallelTest", "integration-test")
        }

        doFirst {
            b.updateSystemProperties(systemProperties)
        }
    }
    return unitTestTask
}

internal fun Project.registerCleanupTask(b: InternalSparkVariantBuild): TaskProvider<Delete> =
    tasks.register<Delete>("cleanUpSparkDirsFor${b.name}") {
        group = "aux"
        description = "Cleans up the Spark directories for variant: ${b.name}"
        delete(b.prop(InternalSparkVariantBuild.DERBY_SYSTEM_HOME))
        delete(b.prop(InternalSparkVariantBuild.SPARK_WAREHOUSE))
    }

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

internal fun Project.registerAggregateUnitTestTask() {
    tasks.register("executeAllVariantsUnitTests") {
        group = "aggregate"
        description = "Aggregate task to run all Spark variant tests"
        dependsOn(
            tasks.withType<Test>().matching { it.group == "unitTests" && it.name != this.name })
    }
}

internal fun Project.registerAggregateJarTask() {
    tasks.register("executeAllVariantsJarTasks") {
        group = "aggregate"
        description = "Aggregate task to assemble all Spark variant jar archives"
        dependsOn(
            tasks.withType<Jar>().matching { it.group == "build" && it.name.endsWith("Jar") })
    }
}

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

internal fun Project.registerAggregateCompileMainSourcesTask() {
    tasks.register("compileAllVariantsJava") {
        group = "aggregate"
        description = "Aggregate task to compile all Spark variants"
        dependsOn(
            tasks.withType<JavaCompile>()
                .filter { it.name.startsWith("compileSpark") })
    }
}

internal fun Project.registerAggregateCompileTestSourcesTask() {
    tasks.register("compileAllVariantsTestJava") {
        group = "aggregate"
        description = "Aggregate task to compile all Spark test variants"
        dependsOn(
            tasks.withType<JavaCompile>()
                .filter { it.name.startsWith("compileTestSpark") })
    }
}

internal fun Project.registerAggregateIntegrationTestTask() {
    tasks.register("executeAllVariantsIntegrationTests") {
        group = "aggregate"
        description = "Aggregate task to run all Spark variant integration tests"
        dependsOn(
            tasks.withType<Test>().matching { it.group == "integrationTests" })
    }
}
