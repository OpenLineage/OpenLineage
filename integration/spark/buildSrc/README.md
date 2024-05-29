# OpenLineage Gradle Plugins

This repository hosts a suite of Gradle plugins designed for configuring and managing the build
process for OpenLineage projects. Written in Kotlin, these plugins are optimized for use with
Gradle.

## Scala213VariantPlugin

Configures the project to support builds that use libraries compiled with Scala 2.13, setting up
necessary source sets, tasks, and configurations for building and testing said code.

To apply this plugin, add the following to your `build.gradle.kts` (or `build.gradle`):

```kotlin
plugins {
    id("io.openlineage.scala213-variant")
}
```

## CommonConfigPlugin

This versatile plugin combines the functionalities of the
former `StandardSpotlessPlugin`, `PrintSourceSetConfigurationPlugin`, and `CommonJavaConfigPlugin`.
It applies common configurations to Java projects and includes additional features like PMD, Lombok,
and Spotless setup.

Features:

- **PMD**: Integrated code analysis tool (configurable).
- **Lombok**: Simplifies Java code, reducing boilerplate.
- **Spotless**: Applies code formatting with Google Java Format and disallows wildcard imports.
- **Print Source Set Configuration**: Registers a task to print the configuration of the project's
  source sets.
- **Java Configuration**: Sets source and target compatibility to Java 8 and configures Maven
  Central, Maven Local, and a custom Astronomer repository.

To apply this plugin, add the following to your `build.gradle.kts` (or `build.gradle`):

```kotlin
plugins {
    id("io.openlineage.common-config")
}
```

To execute the `printSourceSetConfiguration` task, use the following command:

```bash
./gradlew printSourceSetConfiguration
```

## JarVerificationPlugin

This plugin verifies jar prepared with `shadowJar` task. Currently, it checks: 

 * If all `.java` files and classes within them are contained within the jar. This is useful as Spark integration contains several subprojects, and it's easy to forget packing subproject's classes to published jar.
 * If all external classes are relocated (shaded) properly, allowing only specified packages remain unshaded. 

Plugin shall be applied only once in `integration/spark/build.gradle`:

```kotlin
plugins {
  id("io.openlineage.jar-verification")
}
```

Please refer to `JarVerificationPluginExtension` for detailed configuration description.