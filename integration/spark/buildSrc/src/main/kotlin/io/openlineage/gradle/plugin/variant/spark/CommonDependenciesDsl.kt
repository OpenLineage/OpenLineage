/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.artifacts.Dependency
import org.gradle.api.artifacts.ExternalModuleDependency
import org.gradle.api.artifacts.dsl.DependencyHandler
import org.gradle.kotlin.dsl.project

/**
 * Provides a Domain-Specific Language (DSL) for configuring common dependencies across all variant builds created by the SparkVariantsBuildPlugin.
 * This enables defining dependencies once and applying them across multiple variant builds, ensuring consistent dependency versions
 * and configurations. This class is particularly useful when working with a project that needs to compile and test against multiple
 * versions of Apache Spark and Scala, as it simplifies the management of dependencies common to all variants.
 *
 * @property builds A container of the internal representations of each build variant, allowing the common dependencies to be applied to each.
 * @property dependencies The Gradle DependencyHandler used to declare dependencies. It provides a way to add or manipulate the dependencies of a project.
 */
class CommonDependenciesDsl(
    private val builds: NamedDomainObjectContainer<InternalSparkVariantBuild>,
    private val dependencies: DependencyHandler
) {
    fun project(path: String) = dependencies.project(path)

    fun project(path: String, configuration: String) = dependencies.project(path, configuration)

    fun implementation(dependency: Dependency) =
        addToAll(ConfigurationType.IMPLEMENTATION, dependency)

    fun implementation(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.IMPLEMENTATION, dependency, configure)

    fun implementation(dependencyNotation: String) =
        addToAll(ConfigurationType.IMPLEMENTATION, dependencyNotation)

    fun implementation(dependencyNotation: String, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.IMPLEMENTATION, dependencyNotation, configure)

    fun compileOnly(dependency: Dependency) = addToAll(ConfigurationType.COMPILE_ONLY, dependency)

    fun compileOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.COMPILE_ONLY, dependency, configure)

    fun compileOnly(dependencyNotation: String) =
        addToAll(ConfigurationType.COMPILE_ONLY, dependencyNotation)

    fun compileOnly(dependencyNotation: String, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.COMPILE_ONLY, dependencyNotation, configure)

    fun runtimeOnly(dependency: Dependency) = addToAll(ConfigurationType.RUNTIME_ONLY, dependency)

    fun runtimeOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.RUNTIME_ONLY, dependency, configure)

    fun runtimeOnly(dependencyNotation: String) =
        addToAll(ConfigurationType.RUNTIME_ONLY, dependencyNotation)

    fun runtimeOnly(dependencyNotation: String, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.RUNTIME_ONLY, dependencyNotation, configure)

    fun testImplementation(dependency: Dependency) =
        addToAll(ConfigurationType.TEST_IMPLEMENTATION, dependency)

    fun testImplementation(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.TEST_IMPLEMENTATION, dependency, configure)

    fun testImplementation(dependencyNotation: String) =
        addToAll(ConfigurationType.TEST_IMPLEMENTATION, dependencyNotation)

    fun testImplementation(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        addToAll(ConfigurationType.TEST_IMPLEMENTATION, dependencyNotation, configure)

    fun testCompileOnly(dependency: Dependency) =
        addToAll(ConfigurationType.TEST_COMPILE_ONLY, dependency)

    fun testCompileOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.TEST_COMPILE_ONLY, dependency, configure)

    fun testCompileOnly(dependencyNotation: String) =
        addToAll(ConfigurationType.TEST_COMPILE_ONLY, dependencyNotation)

    fun testCompileOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        addToAll(ConfigurationType.TEST_COMPILE_ONLY, dependencyNotation, configure)

    fun testRuntimeOnly(dependency: Dependency) =
        addToAll(ConfigurationType.TEST_RUNTIME_ONLY, dependency)

    fun testRuntimeOnly(dependency: Dependency, configure: ExternalModuleDependency.() -> Unit) =
        addToAll(ConfigurationType.TEST_RUNTIME_ONLY, dependency, configure)

    fun testRuntimeOnly(dependencyNotation: String) =
        addToAll(ConfigurationType.TEST_RUNTIME_ONLY, dependencyNotation)

    fun testRuntimeOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        addToAll(ConfigurationType.TEST_RUNTIME_ONLY, dependencyNotation, configure)

    fun integrationTestRuntimeOnly(dependency: Dependency) =
        addToAll(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependency)

    fun integrationTestRuntimeOnly(
        dependency: Dependency,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        addToAll(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependency, configure)

    fun integrationTestRuntimeOnly(dependencyNotation: String) =
        addToAll(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependencyNotation)

    fun integrationTestRuntimeOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        addToAll(ConfigurationType.INTEGRATION_TEST_RUNTIME_ONLY, dependencyNotation, configure)

    fun integrationTestMountOnly(dependency: Dependency) =
        addToAll(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependency)

    fun integrationTestMountOnly(
        dependency: Dependency,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        addToAll(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependency, configure)

    fun integrationTestMountOnly(dependencyNotation: String) =
        addToAll(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependencyNotation)

    fun integrationTestMountOnly(
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) =
        addToAll(ConfigurationType.INTEGRATION_TEST_UPLOAD_ONLY, dependencyNotation, configure)

    private fun addToAll(cfg: ConfigurationType, dependency: Dependency) = builds.forEach { build ->
        build.configure {
            dependencies {
                add(cfg, dependency)
            }
        }
    }

    private fun addToAll(cfg: ConfigurationType, dependencyNotation: String) =
        builds.forEach { build ->
            build.configure {
                dependencies {
                    add(cfg, dependencyNotation)
                }
            }
        }

    private fun addToAll(
        cfg: ConfigurationType,
        dependency: Dependency,
        configure: ExternalModuleDependency.() -> Unit
    ) = builds.forEach { build ->
        build.configure {
            dependencies {
                add(cfg, dependency) {
                    configure.invoke(this)
                }
            }
        }
    }

    private fun addToAll(
        cfg: ConfigurationType,
        dependencyNotation: String,
        configure: ExternalModuleDependency.() -> Unit
    ) = builds.forEach { build ->
        build.configure {
            dependencies {
                add(cfg, dependencyNotation) {
                    configure.invoke(this)
                }
            }
        }
    }
}
