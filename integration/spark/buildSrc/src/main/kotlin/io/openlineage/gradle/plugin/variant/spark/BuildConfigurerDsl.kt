/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

/**
 * Provides a DSL for configuring a build variant within the SparkVariantsBuildPlugin.
 * This DSL allows specifying dependencies and adjusting settings specific to the build variant,
 * enabling fine-grained control over the configuration of build variant environments.
 *
 * @property build The internal representation of the build variant being configured.
 */
class BuildConfigurerDsl(private val build: InternalSparkVariantBuild) {

    /**
     * The version of Apache Spark used in this build variant.
     */
    val sparkVersion by build::sparkVersion

    /**
     * The version of Scala binary used in this build variant.
     */
    val scalaBinaryVersion by build::scalaBinaryVersion

    /**
     * Configures dependencies for this build variant. Provides a DSL for specifying the dependencies
     * in different scopes such as implementation, compileOnly, runtimeOnly, etc.
     *
     * @param action A lambda expression providing a [DependencyHandlerDsl] to specify dependencies for this build variant.
     */
    fun dependencies(action: DependencyHandlerDsl.() -> Unit) {
        val dsl = DependencyHandlerDsl(build)
        action.invoke(dsl)
    }

    /**
     * Adjusts the settings for this build variant. Through this method, tests can be enabled or disabled,
     * and additional testing options can be configured.
     *
     * @param action A lambda expression providing a [BuildSettingsDsl] to adjust settings specific to this build variant.
     */
    fun settings(action: BuildSettingsDsl.() -> Unit) {
        val dsl = BuildSettingsDsl(build)
        action.invoke(dsl)
    }
}
