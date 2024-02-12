package io.openlineage.gradle.plugin.variant

import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer

internal class BuildVariantDelegate(
    val p: Project,
    val name: String,
    val sparkVersion: String,
    val scalaBinaryVersion: String
) {
    private val configs = p.configurations
    private val sourceSets = p.extensions.getByType(SourceSetContainer::class.java)

    private fun createSourceSetName(
        sparkVersion: String,
        scalaBinaryVersion: String
    ): Pair<String, String> {
        val mainSourceSetName = createMainSourceSetName(sparkVersion, scalaBinaryVersion)
        val testSourceSetName = createTestSourceSetName(sparkVersion, scalaBinaryVersion)
        return Pair(mainSourceSetName, testSourceSetName)
    }

    private fun createMainSourceSetName(
        sparkVersion: String,
        scalaBinaryVersion: String
    ): String {
        return "spark${sparkVersion.replace(".", "")}Scala${scalaBinaryVersion.replace(".", "")}"
    }

    private fun createTestSourceSetName(
        sparkVersion: String,
        scalaBinaryVersion: String
    ): String {
        return "testSpark${sparkVersion.replace(".", "")}Scala${scalaBinaryVersion.replace(".", "")}"
    }

    internal fun mainSourceSet() = sourceSets.getByName(createMainSourceSetName(sparkVersion, scalaBinaryVersion))
    internal fun testSourceSet() = sourceSets.getByName(createTestSourceSetName(sparkVersion, scalaBinaryVersion))

    internal fun configureVariant() {
        val (main, _) = createSourceSets(
            sparkVersion,
            scalaBinaryVersion
        )
        createConfigurations(main)
        registerArtifacts(main)
    }

    private fun registerArtifacts(main: SourceSet) {
        p.artifacts.add(
            main.runtimeElementsConfigurationName,
            p.tasks.getByName(main.jarTaskName).outputs.files.singleFile
        )
    }

    private fun createSourceSets(
        sparkVersion: String,
        scalaBinaryVersion: String
    ): Pair<SourceSet, SourceSet> {
        val (mainSourceSetName, testSourceSetName) = createSourceSetName(
            sparkVersion,
            scalaBinaryVersion
        )
        val sourceSetDelegate = SourceSetCreatorDelegate(p, mainSourceSetName, testSourceSetName)
        return sourceSetDelegate.createSourceSets()
    }

    private fun createConfigurations(main: SourceSet) {
        val configurationDelegate = ConfigurationCreatorDelegate(configs, main)
        configurationDelegate.createConfigurations()
    }

    internal fun setAsDefault() {
        val (mainVariantName, testVariantName) = createSourceSetName(
            sparkVersion,
            scalaBinaryVersion
        )
        val mainVariant = sourceSets.getByName(mainVariantName)
        val main = sourceSets.getByName("main")
        doSwitch(main, mainVariant)
        val testVariant = sourceSets.getByName(testVariantName)
        val test = sourceSets.getByName("test")
        doSwitch(test, testVariant)
    }

    private fun doSwitch(original: SourceSet, variant: SourceSet) {
        original.compileClasspath = variant.compileClasspath
        original.runtimeClasspath = variant.runtimeClasspath
    }
}
