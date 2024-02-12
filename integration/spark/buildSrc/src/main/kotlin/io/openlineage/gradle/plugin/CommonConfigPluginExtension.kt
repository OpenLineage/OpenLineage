/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin

import org.gradle.api.provider.Property

/**
 * Extension for the [CommonConfigPlugin].
 * Allows configuration of various aspects like PMD, Lombok, and Spotless settings.
 *
 * Usage:
 * ```
 * commonConfig {
 *     lombokVersion.set("1.18.30")
 * }
 * ```
 */
interface CommonConfigPluginExtension {
    val lombokVersion: Property<String>
}
