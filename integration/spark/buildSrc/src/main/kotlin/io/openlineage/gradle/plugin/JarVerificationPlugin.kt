/**
 * Copyright 2018-2025 contributors to the OpenLineage project
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.gradle.plugin

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.api.tasks.testing.Test
import org.gradle.configurationcache.extensions.capitalized
import org.gradle.kotlin.dsl.create
import org.gradle.kotlin.dsl.getByType
import org.gradle.kotlin.dsl.register
import java.io.File
import java.util.*

/**
 * A Gradle plugin for verifying content of a jar built. It can validate if specified classes are
 * shaded, it verifies if certain classes are present, and can make sure that only given packages
 * are available in the jar to guarantee jar does not contain non-shaded dependencies.
 */
class JarVerificationPlugin : Plugin<Project> {

    private fun getPluginExtension(target: Project): JarVerificationPluginExtension =
        target.extensions.getByType<JarVerificationPluginExtension>()

    private fun configureExtension(target: Project) {
       target.extensions.create<JarVerificationPluginExtension>("jar")
            .apply {
                packageName.set("io.openlineage.spark")
                relocatePrefix.set("io.openlineage.spark.shaded")
                assertJarContainsAllClasses.convention(true)
                allowedUnshadedPackages.set(listOf(
                    "io.openlineage.client",
                    "shaded.io.openlineage.sql",
                    "io.micrometer.core",
                    "io.micrometer.common",
                    "io.micrometer.observation"
                ))
                highestMajorClassVersionAllowed.set(52)
            }
    }

    override fun apply(target: Project) {
        configureExtension(target)
        target.task("jarVerification") {
            dependsOn("shadowJar", "jar")
            outputs.dir("build/jar-verifier")
            inputs.files(target.tasks.named("shadowJar").get().outputs)
            doLast {
                JarVerificationPluginDelegate(
                    target = target,
                    extension = getPluginExtension(target)
                ).verify()
            }
            group = "verification"
            description = "Verifies content of the jar produced."
        }
    }
}