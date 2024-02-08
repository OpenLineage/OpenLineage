/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.Project

/**
 * An extension for the [SparkVariantsBuildPlugin] allowing detailed configuration of variant builds.
 * This includes adding new variant builds, specifying common dependencies across all builds,
 * and selecting a default build configuration.
 *
 * @property p The Gradle [Project] that this extension is applied to, enabling access to project-specific functionalities and configurations.
 * @constructor Creates a new instance of the extension, initialized with a specific Gradle [Project].
 */
abstract class SparkVariantsBuildPluginExtension @javax.inject.Inject constructor(private val p: Project) {
    private val builds = p.objects.domainObjectContainer(InternalSparkVariantBuild::class.java)
    private val objects = p.objects

    private fun addBuild(build: SparkVariantBuild): InternalSparkVariantBuild {
        val internalBuildVariant = p.configureBuild(build)
        builds.add(internalBuildVariant)
        return internalBuildVariant
    }

    /**
     * Adds a new variant build configuration with the specified name, Apache Spark version, and Scala binary version.
     * This variant is then initialized and registered within the plugin's extension for further configuration
     * and usage in the build process.
     *
     * @param name The unique name of the build variant.
     * @param sparkVersion The version of Apache Spark to be used in this build variant.
     * @param scalaBinaryVersion The Scala binary version to be used in this build variant.
     * @return The [InternalSparkVariantBuild] instance representing the added build variant,
     *         allowing further configuration and use.
     */
    fun add(
        name: String,
        sparkVersion: String,
        scalaBinaryVersion: String
    ): InternalSparkVariantBuild {
        val build =
            objects.newInstance(
                SparkVariantBuild::class.java,
                name,
                sparkVersion,
                scalaBinaryVersion
            )
        return addBuild(build)
    }

    /**
     * Configures a set of common dependencies for all variant builds defined within the extension. This allows
     * for consistent dependency management across different Spark and Scala versions, ensuring that all variants
     * are compiled and tested against the same set of dependencies.
     *
     * @param configure A lambda expression providing a DSL [CommonDependenciesDsl] for defining common dependencies.
     */
    fun commonDependencies(configure: CommonDependenciesDsl.() -> Unit) {
        val dsl = CommonDependenciesDsl(builds, p.dependencies)
        configure.invoke(dsl)
    }

    /**
     * Specifies a default build variant by its name. This variant is used as the primary build configuration
     * unless overridden. This method ensures that a specified default exists among the configured build variants.
     *
     * @param name The name of the build variant to be set as the default.
     * @throws IllegalArgumentException if no build variant with the specified name is found.
     */
    fun setDefaultBuild(name: String) {
        val build = builds.findByName(name)
        check(build != null) { "No build found with name: $name" }
        p.configureDefaultConfigurations(build)
    }
}
