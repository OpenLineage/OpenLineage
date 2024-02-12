package io.openlineage.gradle.plugin.variant

import org.gradle.api.NamedDomainObjectContainer
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet
import javax.inject.Inject

abstract class VariantPluginExtension @Inject constructor(private val p: Project) {
    private val variants = p.objects.domainObjectContainer(BuildVariant::class.java)
    private var activeVariant: BuildVariant? = null

    private fun name(sparkVersion: String, scalaBinaryVersion: String) =
        "spark${sparkVersion.replace(".", "")}Scala${scalaBinaryVersion.replace(".", "")}"

    fun addVariant(sparkVersion: String, scalaBinaryVersion: String) {
        val name = name(sparkVersion, scalaBinaryVersion)
        val buildVariant = p.objects.newInstance(BuildVariant::class.java, name, p)
        variants.add(buildVariant)
        buildVariant.configureVariant(sparkVersion, scalaBinaryVersion)
    }

    fun setActive(sparkVersion: String, scalaBinaryVersion: String) {
        val name = name(sparkVersion, scalaBinaryVersion)
        val variant = variants.getByName(name)
        variant.setAsDefault()
        activeVariant = variant
    }

    fun getActive(): BuildVariant {
        return activeVariant!!
    }

    fun getVariants(): NamedDomainObjectContainer<BuildVariant> = variants

    fun getMainSourceSets(): List<SourceSet> = variants.map { it.mainSourceSet() }

    fun getTestSourceSets(): List<SourceSet> = variants.map { it.testSourceSet() }
}
