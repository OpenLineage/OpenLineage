/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

/**
 * Represents a configuration for a specific Spark and Scala version combination.
 * This class is used to define variant builds as per different combinations of
 * Apache Spark and Scala binary versions.
 *
 * @property name The unique name given to this build variant.
 * @property sparkVersion The version of Apache Spark this variant is targeting.
 * @property scalaBinaryVersion The Scala binary version this variant is using.
 * @constructor Initializes a variant build with the provided name, Spark version, and Scala binary version.
 */
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
