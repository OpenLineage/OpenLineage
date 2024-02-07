/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

class BuildSettingsDsl(private val build: InternalSparkVariantBuild) {
    val sparkVersion by build::sparkVersion
    val scalaBinaryVersion by build::scalaBinaryVersion

    fun disableDeltaTests() {
        build.prop(InternalSparkVariantBuild.DELTA_TESTS_ENABLED, false)
    }

    fun disableIcebergTests() {
        build.prop(InternalSparkVariantBuild.ICEBERG_TESTS_ENABLED, false)
    }

    fun integrationTest(action: IntegrationTestSettingsDsl.() -> Unit) {
        val dsl = IntegrationTestSettingsDsl(build)
        action.invoke(dsl)
    }

    /**
     * This particular method is to allow some flexibility when configuring the system properties for tests.
     * The use case for this is if one test suite has a very specific configuration that is not shared by the rest
     * then this method can be used to enable that bespoke configuration.
     *
     * Example:
     *
     * Suppose you have a test method that you want to guard behind a custom system property.
     *
     * ```java
     * @Test
     * @EnabledIfSystemProperty(named = "my.custom.property", matches = "true")
     * void testSomeNewFeature() {
     *   SparkSession spark = SparkSession.builder().getOrCreate();
     *   // do some stuff, assert some things, etc
     * }
     * ```
     *
     * You can use this method whilst configuring a build to add that property, which will then be set. In your build script
     * ```kotlin
     * builds {
     *     add(...).configure {
     *          settings {
     *              setSystemPropertyForTests("my.custom.property", "true")
     *          }
     *     }
     * }
     * ```
     */
    fun setSystemPropertyForTests(key: String, value: Any) {
        build.prop(key, value)
    }
}
