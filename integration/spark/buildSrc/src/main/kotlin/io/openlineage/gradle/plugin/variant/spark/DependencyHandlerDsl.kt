/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ExternalModuleDependency
import org.gradle.kotlin.dsl.project

class DependencyHandlerDsl(private val b: InternalSparkVariantBuild) {
    private val dependencies = b.dependencies

    val sparkVersion = b.sparkVersion
    val scalaBinaryVersion = b.scalaBinaryVersion

    fun project(path: String) = dependencies.project(path)

    fun project(path: String, configuration: String) = dependencies.project(path, configuration)

    fun implementation(dependency: Dependency) = add(ConfigurationType.IMPLEMENTATION, dependency)

    fun implementation(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.IMPLEMENTATION, dependency, configure)

    fun implementation(dependencyNotation: String) =
        add(ConfigurationType.IMPLEMENTATION, dependencyNotation)

    fun implementation(dependencyNotation: String, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.IMPLEMENTATION, dependencyNotation, configure)

    fun implementation(dependency: Dependency, configuration: String) =
        add(ConfigurationType.IMPLEMENTATION, dependency) {
            this.targetConfiguration = configuration
        }

    fun compileOnly(dependency: Dependency) = add(ConfigurationType.COMPILE_ONLY, dependency)

    fun compileOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.COMPILE_ONLY, dependency, configure)

    fun compileOnly(dependencyNotation: String) =
        add(ConfigurationType.COMPILE_ONLY, dependencyNotation)

    fun compileOnly(dependencyNotation: String, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.COMPILE_ONLY, dependencyNotation, configure)

    fun runtimeOnly(dependency: Dependency) = add(ConfigurationType.RUNTIME_ONLY, dependency)

    fun runtimeOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.RUNTIME_ONLY, dependency, configure)

    fun runtimeOnly(dependencyNotation: String) =
        add(ConfigurationType.RUNTIME_ONLY, dependencyNotation)

    fun runtimeOnly(dependencyNotation: String, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.RUNTIME_ONLY, dependencyNotation, configure)

    fun testImplementation(dependency: Dependency) =
        add(ConfigurationType.TEST_IMPLEMENTATION, dependency)

    fun testImplementation(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.TEST_IMPLEMENTATION, dependency, configure)

    fun testImplementation(dependencyNotation: String) =
        add(ConfigurationType.TEST_IMPLEMENTATION, dependencyNotation)

    fun testImplementation(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        add(ConfigurationType.TEST_IMPLEMENTATION, dependencyNotation, configure)

    fun testCompileOnly(dependency: Dependency) =
        add(ConfigurationType.TEST_COMPILE_ONLY, dependency)

    fun testCompileOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.TEST_COMPILE_ONLY, dependency, configure)

    fun testCompileOnly(dependencyNotation: String) =
        add(ConfigurationType.TEST_COMPILE_ONLY, dependencyNotation)

    fun testCompileOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        add(ConfigurationType.TEST_COMPILE_ONLY, dependencyNotation, configure)

    fun testRuntimeOnly(dependency: Dependency) =
        add(ConfigurationType.TEST_RUNTIME_ONLY, dependency)

    fun testRuntimeOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        add(ConfigurationType.TEST_RUNTIME_ONLY, dependency, configure)

    fun testRuntimeOnly(dependencyNotation: String) =
        add(ConfigurationType.TEST_RUNTIME_ONLY, dependencyNotation)

    fun testRuntimeOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        add(ConfigurationType.TEST_RUNTIME_ONLY, dependencyNotation, configure)

    fun integrationTestRuntimeOnly(dependency: Dependency) =
        add(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependency)

    fun integrationTestRuntimeOnly(
        dependency: Dependency,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        add(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependency, configure)

    fun integrationTestRuntimeOnly(dependencyNotation: String) =
        add(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependencyNotation)

    fun integrationTestRuntimeOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        add(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependencyNotation, configure)

    fun integrationTestMountOnly(dependency: Dependency) =
        add(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependency)

    fun integrationTestMountOnly(
        dependency: Dependency,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        add(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependency, configure)

    fun integrationTestMountOnly(dependencyNotation: String) =
        add(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependencyNotation)

    fun integrationTestMountOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        add(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependencyNotation, configure)

    internal fun add(cfg: ConfigurationType, dependency: Dependency): Dependency =
        doAdd(cfg, dependency)

    internal fun add(
        cfg: ConfigurationType,
        dependency: Dependency,
        configure: ExternalModuleDependency.() -> Unit
    ): Dependency {
        val dep = doAdd(cfg, dependency)
        configure.invoke(dependency as ExternalModuleDependency)
        return dep
    }

    internal fun add(cfg: ConfigurationType, dependencyNotation: String): Dependency =
        doAdd(cfg, dependencyNotation)

    internal fun add(
        cfg: ConfigurationType,
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ): Dependency {
        val dep = doAdd(cfg, dependencyNotation)
        configure.invoke(dep as ExternalModuleDependency)
        return dep
    }

    private fun doAdd(cfg: ConfigurationType, dependencyNotation: Any): Dependency {
        val configurationName = when (cfg) {
            ConfigurationType.COMPILE_ONLY -> b.mainCompileOnly
            ConfigurationType.RUNTIME_ONLY -> b.mainRuntimeOnly
            ConfigurationType.IMPLEMENTATION -> b.mainImplementation
            ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY -> b.integrationTestRuntimeOnly
            ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY -> b.integrationTestAdditionalJars
            ConfigurationType.TEST_COMPILE_ONLY -> b.testCompileOnly
            ConfigurationType.TEST_RUNTIME_ONLY -> b.testRuntimeOnly
            ConfigurationType.TEST_IMPLEMENTATION -> b.testImplementation
        }

        return dependencies.add(configurationName, dependencyNotation)!!
    }
}
