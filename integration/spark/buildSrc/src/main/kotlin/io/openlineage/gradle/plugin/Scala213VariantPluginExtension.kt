package io.openlineage.gradle.plugin

import org.gradle.api.provider.Property

/**
 * Extension for the [Scala213VariantPlugin].
 * Provides configurable properties specific to the Scala 2.13 environment.
 *
 * Usage:
 * ```
 * scala213 {
 *     archiveBaseName.set("openlineage-spark-${project.name}")
 * }
 * ```
 */
interface Scala213VariantPluginExtension {
    val archiveBaseName: Property<String>
}
