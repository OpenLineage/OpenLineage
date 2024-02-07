/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

internal enum class ConfigurationType {
    COMPILE_ONLY,
    IMPLEMENTATION,
    INTEGRATION_TEST_RUNTIME_ONLY,
    INTEGRATION_TEST_UPLOAD_ONLY,
    RUNTIME_ONLY,
    TEST_COMPILE_ONLY,
    TEST_IMPLEMENTATION,
    TEST_RUNTIME_ONLY,
}
