package io.openlineage.gradle.plugin.variant

import org.gradle.api.Project
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.SourceSetContainer

internal class SourceSetCreatorDelegate(
    p: Project,
    private val mainSourceSetName: String,
    private val testSourceSetName: String
) {
    private val logger = p.logger
    private val sourceSets = p.extensions.getByType(SourceSetContainer::class.java)

    fun createSourceSets(): Pair<SourceSet, SourceSet> {
        logger.info("Creating main source sets {}", mainSourceSetName)
        val mainSourceSet = sourceSets.create(mainSourceSetName).apply {
            java.setSrcDirs(listOf("src/main/java"))
            resources.setSrcDirs(listOf("src/main/resources"))
        }

        logger.info("Creating test source sets {}", testSourceSetName)
        val testSourceSet = sourceSets.create(testSourceSetName).apply {
            java.setSrcDirs(listOf("src/test/java"))
            resources.setSrcDirs(listOf("src/test/resources"))

            compileClasspath += mainSourceSet.output
            runtimeClasspath += mainSourceSet.output
        }

        return Pair(mainSourceSet, testSourceSet)
    }
}
