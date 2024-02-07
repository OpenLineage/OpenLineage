/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

class BuildConfigurerDsl(private val build: InternalSparkVariantBuild) {
    val sparkVersion by build::sparkVersion
    val scalaBinaryVersion by build::scalaBinaryVersion

    fun dependencies(action: DependencyHandlerDsl.() -> Unit) {
        val dsl = DependencyHandlerDsl(build)
        action.invoke(dsl)
    }

    fun settings(action: BuildSettingsDsl.() -> Unit) {
        val dsl = BuildSettingsDsl(build)
        action.invoke(dsl)
    }
}
