/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.Project

abstract class SparkVariantsBuildPluginExtension @javax.inject.Inject constructor(private val p: Project) {
    private val builds = p.objects.domainObjectContainer(InternalSparkVariantBuild::class.java)
    private val objects = p.objects

    private fun addBuild(build: SparkVariantBuild): InternalSparkVariantBuild {
        val internalBuildVariant = p.configureBuild(build)
        builds.add(internalBuildVariant)
        return internalBuildVariant
    }

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

    fun commonDependencies(configure: CommonDependenciesDsl.() -> Unit) {
        val dsl = CommonDependenciesDsl(builds, p.dependencies)
        configure.invoke(dsl)
    }

    fun setDefaultBuild(name: String) {
        val build = builds.findByName(name)
        check(build != null) { "No build found with name: $name" }
        p.configureDefaultConfigurations(build)
    }
}
