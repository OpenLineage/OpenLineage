/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

class IntegrationTestSettingsDsl(private val build: InternalSparkVariantBuild) {
    /**
     * Use a specific docker image for integration tests. This should be the fully qualified form of the image.
     *
     * That is, supply the image name (with its repository, if applicable) and the image tag.
     *
     * For example:
     *
     * ```kotlin
     * integrationTests {
     *      useDockerImage("bitnami/spark:3.5.0")
     * }
     * ```
     *
     * Defaults to `openlineage/spark:spark-${sparkVersion}-scala-${scalaBinaryVersion}`
     */
    fun useDockerImage(image: String) {
        build.prop(InternalSparkVariantBuild.DOCKER_IMAGE_NAME, image)
    }
}
