/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

/**
 * Enumerates the types of Gradle configurations that are utilized within the SparkVariantsBuildPlugin to
 * configure dependencies across various build variants. This includes configurations for compiling, testing,
 * runtime, and integration testing, each tailored to the specifics of both Apache Spark and Scala versions.
 *
 * This enum is used internally by the plugin to streamline the assignment of dependencies to the respective
 * configurations of each build variant, ensuring that all necessary dependencies are correctly applied according
 * to their designated use case (e.g., compilation, runtime, testing).
 */
internal enum class ConfigurationType {
    /** Configuration for dependencies that are necessary for compilation but not at runtime. */
    COMPILE_ONLY,
    /** Configuration for dependencies that are directly used in the code and are required at both compile-time and runtime. */
    IMPLEMENTATION,
    /** Configuration specifically for dependencies required at runtime during integration tests. */
    INTEGRATION_TEST_RUNTIME_ONLY,
    /** Configuration for additional dependencies that need to be uploaded for integration tests but are not part of the runtime classpath. */
    INTEGRATION_TEST_UPLOAD_ONLY,
    /** Configuration for dependencies needed at runtime but not for compilation. */
    RUNTIME_ONLY,
    /** Configuration for dependencies that are necessary to compile test sources but not required at runtime. */
    TEST_COMPILE_ONLY,
    /** Configuration for dependencies used in the test source set and required at both compile-time and runtime for testing. */
    TEST_IMPLEMENTATION,
    /** Configuration for dependencies required at runtime only when running tests. */
    TEST_RUNTIME_ONLY,
}
