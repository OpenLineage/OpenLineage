/**
 * Copyright 2018-2024 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin

import org.gradle.api.provider.ListProperty
import org.gradle.api.provider.Property
import java.util.Optional

/**
 * Extension for the [JarVerificationPlugin].
 * Allows configuring how the shadow jar shall be verified
 *
 * Usage:
 * ```
 * shadowJarVerification {
 *     packageName("io.openlineage.spark")
 *     relocatePrefix.set("io.openlineage.spark.shaded")
 *     allowedUnshadedPackages.set(listOf(
 *          "io.micrometer.common"
 *     ))
 *     assertJarContainsAllClasses.set(true)
 *     assertJarDoesNotContainUnshadedClasses.set(true)
 * }
 * ```
 */
interface JarVerificationPluginExtension {

    /**
     * Expected package name for the source classes
     */
    val packageName: Property<String>

    /**
     * Prefix configured within shading plugin to relocate bundled dependencies into packages
     * with prefixed package names.
     */
    val relocatePrefix: Property<String>

    /**
     * List of packages that are allowed to be unshaded and contained within jar.
     */
    val allowedUnshadedPackages: ListProperty<String>

    /**
     * If set to true, plugin will check if java classes available in the project (without dependency
     * classes) are present. This will allow making sure that JAR content contains compiled classes.
     */
    val assertJarContainsAllClasses: Property<Boolean>

    /**
     * If set to true, shadow jar should only contain classes from within the built project's
     * package and classes with contained within shadingPrefix directories.
     *
     * Exceptions to this rule can be defined in allowedUnshadedPackages
     */
    val assertJarDoesNotContainUnshadedClasses: Property<Boolean>
}
