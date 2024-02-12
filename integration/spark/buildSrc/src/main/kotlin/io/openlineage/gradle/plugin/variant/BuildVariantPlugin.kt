package io.openlineage.gradle.plugin.variant

import org.gradle.api.Plugin
import org.gradle.api.Project
import org.gradle.api.plugins.JavaLibraryPlugin

class BuildVariantPlugin : Plugin<Project> {
    override fun apply(project: Project): Unit = with(project) {
        plugins.withType(JavaLibraryPlugin::class.java) {
            extensions.create("buildVariants", VariantPluginExtension::class.java)
        }
    }
}
