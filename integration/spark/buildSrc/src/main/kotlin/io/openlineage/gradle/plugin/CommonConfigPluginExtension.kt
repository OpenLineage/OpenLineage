/*
* SPDX-License-Identifier: Apache-2.0
* Copyright 2018-2023 contributors to the OpenLineage project
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
 *     pmdEnabled.set(true)
 *     lombokEnabled.set(true)
 *     lombokVersion.set("1.18.30")
 *     spotlessEnabled.set(true)
 * }
 * ```
 */
interface CommonConfigPluginExtension {
    val pmdEnabled: Property<Boolean>
    val lombokEnabled: Property<Boolean>
    val lombokVersion: Property<String>
    val spotlessEnabled: Property<Boolean>
}
