/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin.variant.spark

/**
 * Provides a DSL for configuring integration test settings specific to a build variant. Through this DSL, users
 * can customize aspects related to integration testing, such as specifying a custom Docker image to use during
 * the tests. This allows for a more tailored and flexible setup when running integration tests against various
 * Spark and Scala versions.
 *
 * @property build The [InternalSparkVariantBuild] instance for which the integration test settings are being configured.
 */
class IntegrationTestSettingsDsl(private val build: InternalSparkVariantBuild) {
    /**
     * Specifies a custom Docker image to be used for running integration tests. By default, the plugin uses a
     * generated Docker image name based on Spark and Scala versions. This method allows overriding that image
     * with any valid Docker image name, providing flexibility in the testing environment.
     *
     * Example of usage:
     * ```kotlin
     * integrationTests {
     *      useDockerImage("my-custom-image:latest")
     * }
     * ```
     *
     * @param image The name of the Docker image to be used for integration tests, including the tag.
     */
    fun useDockerImage(image: String) {
        build.prop(InternalSparkVariantBuild.DOCKER_IMAGE_NAME, image)
    }
}
