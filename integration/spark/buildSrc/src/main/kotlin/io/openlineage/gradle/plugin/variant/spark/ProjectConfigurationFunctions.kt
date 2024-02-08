/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.Project
import org.gradle.api.tasks.SourceSetContainer


internal fun Project.configureBuild(sparkVariantBuild: SparkVariantBuild): InternalSparkVariantBuild {
    val internalSparkVariantBuild = configureSourceSets(sparkVariantBuild)
    configureConfigurations(internalSparkVariantBuild)
    registerTasks(internalSparkVariantBuild)

    return internalSparkVariantBuild
}

/**
 * Configures the source sets for a given Spark and Scala build variants. It creates new source sets for the main
 * and test code, based on the provided [SparkVariantBuild] instance. These source sets mirror the default
 * 'main' and 'test' source sets but are named according to Spark and Scala versions.
 *
 * @receiver The Gradle [Project] within which the source sets are being configured.
 * @param b The [SparkVariantBuild] instance representing the specific Spark and Scala version
 * combination for which the source sets are being created.
 * @return An [InternalSparkVariantBuild] instance containing details of the configured source sets.
 */
internal fun Project.configureSourceSets(b: SparkVariantBuild): InternalSparkVariantBuild {
    val sourceSets = extensions.getByType(SourceSetContainer::class.java)
    val main = sourceSets.getByName("main")
    val test = sourceSets.getByName("test")

    val mainSourceSet = sourceSets.create(b.mainSourceSetName) {
        java.setSrcDirs(main.java.srcDirs)
        resources.setSrcDirs(main.resources.srcDirs)
    }

    val testSourceSet = sourceSets.create(b.testSourceSetName) {
        java.setSrcDirs(test.java.srcDirs)
        resources.setSrcDirs(test.resources.srcDirs)
        compileClasspath += mainSourceSet.output
        runtimeClasspath += mainSourceSet.output
    }

    val objects = project.objects
    val internalVariant =
        objects.newInstance(
            InternalSparkVariantBuild::class.java,
            b.name,
            b.sparkVersion,
            b.scalaBinaryVersion,
            project,
            mainSourceSet,
            testSourceSet
        )

    return internalVariant
}

/**
 * Configures the Gradle configurations required for the built variant represented by [InternalSparkVariantBuild].
 * This includes setting up the compilations, runtime, and other necessary configurations for the build variant.
 *
 * @receiver The Gradle [Project] within which the configurations are being set up.
 * @param b An instance of [InternalSparkVariantBuild] representing the build variant for which the configurations
 * are being defined.
 */
internal fun Project.configureConfigurations(b: InternalSparkVariantBuild) {
    val mainCompileOnlyApi = configurations.maybeCreate(b.mainCompileOnlyApi)
    mainCompileOnlyApi.isCanBeConsumed = false
    mainCompileOnlyApi.isCanBeResolved = false

    val mainApi = configurations.maybeCreate(b.mainApi)
    mainApi.isCanBeConsumed = false
    mainApi.isCanBeResolved = false

    val mainCompileOnly = configurations.maybeCreate(b.mainCompileOnly)
    mainCompileOnly.isCanBeConsumed = false
    mainCompileOnly.isCanBeResolved = false
    mainCompileOnly.extendsFrom(mainCompileOnlyApi)

    val mainApiElements = configurations.maybeCreate(b.mainApiElements)
    mainApiElements.isCanBeConsumed = true
    mainApiElements.isCanBeResolved = false
    mainApiElements.extendsFrom(mainApi, mainCompileOnlyApi)

    val mainImplementation = configurations.maybeCreate(b.mainImplementation)
    mainImplementation.isCanBeConsumed = false
    mainImplementation.isCanBeResolved = false
    mainImplementation.extendsFrom(mainApi)

    val mainRuntimeOnly = configurations.maybeCreate(b.mainRuntimeOnly)
    mainImplementation.isCanBeConsumed = false
    mainImplementation.isCanBeResolved = false

    val mainCompileClassPath = configurations.maybeCreate(b.mainCompileClassPath)
    mainCompileClassPath.isCanBeConsumed = false
    mainCompileClassPath.isCanBeResolved = true
    mainCompileClassPath.extendsFrom(mainCompileOnly, mainImplementation)

    val mainRuntimeElements = configurations.maybeCreate(b.mainRuntimeElements)
    mainRuntimeElements.isCanBeConsumed = true
    mainRuntimeElements.isCanBeResolved = false
    mainRuntimeElements.extendsFrom(mainImplementation, mainRuntimeOnly)

    val mainRuntimeClasspath = configurations.maybeCreate(b.mainRuntimeClassPath)
    mainRuntimeClasspath.isCanBeConsumed = false
    mainRuntimeClasspath.isCanBeResolved = true
    mainRuntimeClasspath.extendsFrom(mainImplementation, mainRuntimeOnly)

    val testCompileOnly = configurations.maybeCreate(b.testCompileOnly)
    testCompileOnly.extendsFrom(mainCompileOnlyApi)

    val testImplementation = configurations.maybeCreate(b.testImplementation)
    testImplementation.extendsFrom(mainImplementation)

    val testRuntimeOnly = configurations.maybeCreate(b.testRuntimeOnly)
    testRuntimeOnly.extendsFrom(mainRuntimeOnly)

    val testCompileClasspath = configurations.maybeCreate(b.testCompileClasspath)
    testCompileClasspath.isCanBeResolved = true
    testCompileClasspath.extendsFrom(testCompileOnly, testImplementation)

    val testRuntimeClasspath = configurations.maybeCreate(b.testRuntimeClasspath)
    testRuntimeClasspath.isCanBeResolved = true
    testRuntimeClasspath.extendsFrom(testRuntimeOnly, testImplementation)

    // Now, we create four configurations meant for the integration test tasks
    // The first one will define the dependencies that we need to run the integration tests, i.e., testcontainers.
    // The second is the runtime classpath for the integration test task. This needs to be resolvable.
    // The third is what we will mount to the container as additional runtime dependencies
    // THe fourth is for test fixtures, that we will also mount to the counteiner
    val integrationTestRuntimeOnly = configurations.maybeCreate(b.integrationTestRuntimeOnly)
    integrationTestRuntimeOnly.isCanBeResolved = false
    integrationTestRuntimeOnly.isCanBeConsumed = false

    val integrationTestRuntimeClasspath =
        configurations.maybeCreate(b.integrationTestRuntimeClasspath)
    integrationTestRuntimeClasspath.extendsFrom(integrationTestRuntimeOnly, testImplementation)
    integrationTestRuntimeClasspath.isCanBeResolved = true
    integrationTestRuntimeClasspath.isCanBeConsumed = false

    val integrationTestAdditionalJars = configurations.maybeCreate(b.integrationTestAdditionalJars)
    integrationTestAdditionalJars.isCanBeResolved = true
    integrationTestAdditionalJars.isCanBeConsumed = false

    val integrationTestFixtures = configurations.maybeCreate(b.integrationTestFixtures)
    integrationTestFixtures.isCanBeResolved = true
    integrationTestFixtures.isCanBeConsumed = false
}

/**
 * Configures the default configurations of the project to extend those of a specified internal Spark variant build.
 * This essentially makes the specified build the default build, integrating its configurations with the project’s
 * default ones.
 *
 * @receiver The Gradle [Project] in which the default configurations are being adjusted.
 * @param b An instance of [InternalSparkVariantBuild] representing the build variant to be used as the default.
 */
internal fun Project.configureDefaultConfigurations(b: InternalSparkVariantBuild) {
    configurations.named("implementation").configure {
        extendsFrom(configurations.getByName(b.mainImplementation))
    }
    configurations.named("compileOnly").configure {
        extendsFrom(configurations.getByName(b.mainCompileOnly))
    }
    configurations.named("runtimeOnly").configure {
        extendsFrom(configurations.getByName(b.mainRuntimeOnly))
    }
    configurations.named("testImplementation").configure {
        extendsFrom(configurations.getByName(b.testImplementation))
    }
    configurations.named("testCompileOnly").configure {
        extendsFrom(configurations.getByName(b.testCompileOnly))
    }
    configurations.named("testRuntimeOnly").configure {
        extendsFrom(configurations.getByName(b.testRuntimeOnly))
    }
}
