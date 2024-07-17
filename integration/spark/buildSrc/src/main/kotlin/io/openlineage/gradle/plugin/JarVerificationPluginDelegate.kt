package io.openlineage.gradle.plugin

import org.gradle.api.GradleException
import org.gradle.api.Project
import org.gradle.api.tasks.SourceSetContainer
import org.gradle.kotlin.dsl.getByType
import org.slf4j.LoggerFactory
import java.io.File
import javassist.bytecode.ClassFile
import java.io.DataInputStream
import java.io.FileInputStream

/**
 * A delegate class to implement jar verification logic. By design, the class methods shall be
 * agnostic to current project specifics which should be configurable.
 */
class JarVerificationPluginDelegate(
    private val target: Project,
    private val extension: JarVerificationPluginExtension
) {
    private fun getSourceSetContainer(target: Project) =
        target.extensions.getByType<SourceSetContainer>()

    val logger = LoggerFactory.getLogger("JarVerificationLogger")

    fun verify() {
        val identifiersFromJar: List<ClassIdentifier> =  classIdentifiersFromJar(target)
        val identifiersFromSource: List<ClassIdentifier> =  classIdentifiersFromSource(target)

        logger.info("Encountered ${identifiersFromSource.size} java source class files")

        if (hasUnshadedPackages(identifiersFromJar)
            .or(areMissingClasses(identifiersFromSource, identifiersFromJar))
            .or(hasCompiledClassesMajorVersionAboveAllowed())
        ) {
            throw GradleException("Jar verification task failed")
        }
    }

    /**
     * Given a list of `ClassIdentifier` from a build jar it verifies the packages contained.
     * It logs an error if encountered package which does not start with configured packageName,
     * does not start with relocate prefix and is not on the list of allowed unshaded packages.
     *
     * Method prints error for each violation and returns boolean with information if any violations
     * have been encountered.
     */
    private fun hasUnshadedPackages(identifiersFromJar: List<ClassIdentifier>): Boolean {
        return identifiersFromJar
            .map { c -> c.packageName }
            .distinct()
            .filter { p -> !p.startsWith(extension.packageName.get()) }
            .filter { p -> !p.startsWith(extension.relocatePrefix.get()) }
            .filter { p ->
                extension
                    .allowedUnshadedPackages
                    .get()
                    .find { allowed -> p.startsWith(allowed) } == null
            }
            .map {
                p -> logger.error("[ERROR] Package '$p' should not be present in jar")
            }
            .isNotEmpty()
    }

    /**
     * Verifies bytecode of all the classes from the Jar. It decodes .class files
     * and extracts major class version property from them. It verifies if major Java version
     * for all the classes is lower or equal to the one configuread as maximal allowed.
     * For example, for Java 8 compiled classes should contain class major version equal to 52.
     */
    private fun hasCompiledClassesMajorVersionAboveAllowed(): Boolean {
        return target
            .zipTree(locateJar(target).toPath())
            .files
            .filter { it.extension == "class" }
            .filter { !it.path.contains("META-INF") }
            .filter { majorVersionOf(it.path) > extension.highestMajorClassVersionAllowed.get() }
            .map { p ->
                logger.error("[ERROR] Major class version for '${p.path}' " +
                        "is '${majorVersionOf(p.path)}', which is above " +
                        "'${extension.highestMajorClassVersionAllowed.get()}'")
            }
            .isNotEmpty()
    }

    private fun majorVersionOf(path: String): Int {
        return ClassFile(DataInputStream(FileInputStream(File(path)))).majorVersion
    }

    /**
     * List of `ClassIdentifier` from the source code is built by walking project root directory
     * and looking for `.java` classes. All elements from that list has to be present in the list
     * of `ClassIdentifier` created from a built jar.
     */
    private fun areMissingClasses(
        fromSource: List<ClassIdentifier>,
        fromJar: List<ClassIdentifier>
    ): Boolean {
        val packedClasses: Map<String, List<String>> = fromJar
            .map { it.packageName to it.className }
            .groupBy({ it.first }, { it.second })

        val missingPackage = fromSource
            .map { it.packageName }
            .distinct()
            .filter { !packedClasses.contains(it) }
            .map { p ->
                logger.error("[ERROR] Source code package '$p' is not present in jar")
            }
            .isNotEmpty()

        val missingClass =
            fromSource
                .filter {
                    !(packedClasses.get(it.packageName)
                        ?.contains(it.className) ?: false)
                }
                .map { p ->
                    logger.error(
                        "[ERROR] Source code class '$p' not present in jar"
                    )
                }
                .isNotEmpty()

        return missingPackage.or(missingClass)
    }

    private fun classIdentifiersFromJar(target: Project): List<ClassIdentifier> {
        return target
            .zipTree(locateJar(target).toPath())
            .files
            .filter { it.extension == "class" }
            .filter { !it.path.contains("META-INF") }
            .map { file ->
                val arr = file.parentFile.path.split(File.separator!!)
                val startPathIndex = arr.indexOfFirst { it.startsWith("zip_") }
                ClassIdentifier(
                    arr.subList(startPathIndex + 1, arr.size).joinToString(separator = ".").lowercase(),
                    file.nameWithoutExtension
                )
            }
    }

    private fun classIdentifiersFromSource(target: Project): List<ClassIdentifier> {
        return target
            .subprojects
            .map { it.name + "/src/main/java"  }
            .flatMap { t -> File(t).walkTopDown().filter { it.extension == "java" } }
            .map { file ->
                val arr = file.parentFile.path.split(File.separator!!)
                val startPathIndex = arr.indexOfFirst { it.equals("java") }
                ClassIdentifier(
                    arr.subList(startPathIndex + 1, arr.size).joinToString(separator = ".").lowercase(),
                    file.nameWithoutExtension
                )
            }
            .toList()
    }

    private fun locateJar(target: Project): File {
        return target
            .tasks
            .named("shadowJar")
            .get()
            .outputs
            .files
            .filter { it.extension == "jar" }
            .find { it.name.contains(target.project.name)  }
            ?: throw GradleException("Couldn't locat3e jar to verify")
    }
}

/**
 * Utilisty class to contain `package` and `class` tuple.
 */
class ClassIdentifier(val packageName: String, val className: String) {
    override fun toString(): String { return "package $packageName, class $className" }
}