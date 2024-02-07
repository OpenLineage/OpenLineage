/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

abstract class SparkVariantBuild @javax.inject.Inject constructor(
    val name: String,
    val sparkVersion: String,
    val scalaBinaryVersion: String
) {
    private val sparkVersionFmt = sparkVersion.removePeriods()
    private val scalaBinaryVersionFmt = scalaBinaryVersion.removePeriods()

    val mainSourceSetName = "mainSpark${sparkVersionFmt}Scala${scalaBinaryVersionFmt}"
    val testSourceSetName = "testSpark${sparkVersionFmt}Scala${scalaBinaryVersionFmt}"
}
