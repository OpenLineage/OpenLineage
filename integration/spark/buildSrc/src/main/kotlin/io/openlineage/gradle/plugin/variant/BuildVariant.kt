package io.openlineage.gradle.plugin.variant

import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet
import javax.inject.Inject

abstract class BuildVariant @Inject constructor(val name: String, private val p: Project) {
    private val logger = p.logger
    private var delegate: BuildVariantDelegate? = null

    fun configureVariant(sparkVersion: String, scalaBinaryVersion: String) {
        val delegate = BuildVariantDelegate(p, name, sparkVersion, scalaBinaryVersion)
        delegate.configureVariant()
        this.delegate = delegate
    }

    val sparkVersion = lazy { delegate!!.sparkVersion }
    val scalaBinaryVersion = lazy { delegate!!.scalaBinaryVersion }

    fun mainSourceSet(): SourceSet = delegate!!.mainSourceSet()

    fun testSourceSet(): SourceSet = delegate!!.testSourceSet()

    fun setAsDefault() {
        logger.info("Setting variant {} as default", name)
        delegate?.setAsDefault()
    }
}
