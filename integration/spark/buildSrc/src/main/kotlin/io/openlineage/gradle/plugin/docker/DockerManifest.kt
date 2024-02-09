/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.docker

import kotlinx.serialization.Serializable

@Serializable
data class DockerManifest(
    val sparkVersion: String,
    val scalaBinaryVersion: String,
    val sparkSourceBinaries: String,
    val sparkSourceBinariesAsc: String,
    val sparkPgpKeys: String,
    val baseDockerImage: String
)
