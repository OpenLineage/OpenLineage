/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

/**
 * Provides a DSL for adjusting settings specific to a build variant. Through this,
 * one can enable or disable certain test sets, or modify properties specific to how the tests are run.
 * This allows for greater customization of the build process on a per-variant basis.
 */
class BuildSettingsDsl(private val build: InternalSparkVariantBuild) {
    /**
     * The version of Apache Spark used in this build variant.
     */
    val sparkVersion by build::sparkVersion

    /**
     * The version of Scala binary used in this build variant.
     */
    val scalaBinaryVersion by build::scalaBinaryVersion

    /**
     * Disables Delta tests for this build variant. This is useful if the variant
     * doesn't support Delta or to speed up testing by skipping tests that are not relevant.
     */
    fun disableDeltaTests() {
        build.prop(InternalSparkVariantBuild.DELTA_TESTS_ENABLED, false)
    }

    /**
     * Disables Iceberg tests for this build variant. Similar to `disableDeltaTests`, this can be
     * used to skip tests that are not applicable, or to reduce test time.
     */
    fun disableIcebergTests() {
        build.prop(InternalSparkVariantBuild.ICEBERG_TESTS_ENABLED, false)
    }

    /**
     * Configures integration test settings for this build variant by applying the specified
     * configuration block to an instance of [IntegrationTestSettingsDsl].
     *
     * @param action The configuration block to adjust settings specific to integration tests.
     */
    fun integrationTest(action: IntegrationTestSettingsDsl.() -> Unit) {
        val dsl = IntegrationTestSettingsDsl(build)
        action.invoke(dsl)
    }

    /**
     * Sets a custom system property to be used during test execution for this build variant.
     * This is useful for controlling test behavior through system properties.
     *
     * Example usage:
     * ```kotlin
     * settings {
     *     setSystemPropertyForTests("my.custom.property", "true")
     * }
     * ```
     * This can then be checked in test code to modify the behavior based on the property value.
     *
     * @param key The system property key.
     * @param value The value associated with the system property.
     */
    fun setSystemPropertyForTests(key: String, value: Any) {
        build.prop(key, value)
    }
}
