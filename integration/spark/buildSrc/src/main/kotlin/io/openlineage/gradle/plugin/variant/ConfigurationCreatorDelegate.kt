package io.openlineage.gradle.plugin.variant

import org.gradle.api.artifacts.ConfigurationContainer
import org.gradle.api.tasks.SourceSet

internal class ConfigurationCreatorDelegate(private val configs: ConfigurationContainer, main: SourceSet) {
    private val mainImplementation = main.implementationConfigurationName
    private val mainRuntimeOnly = main.runtimeOnlyConfigurationName
    private val mainRuntimeElements = main.runtimeElementsConfigurationName

    @Suppress("UNUSED_VARIABLE")
    fun createConfigurations() {
        val implementation = configs.getByName(mainImplementation)
        val runtimeOnly = configs.getByName(mainRuntimeOnly)
        // This configuration is needed to publish the artifacts that a particular project produces
        val runtimeElements = configs.maybeCreate(mainRuntimeElements).apply {
            extendsFrom(implementation, runtimeOnly)
            isCanBeResolved = false
            isCanBeConsumed = true
        }
    }
}
