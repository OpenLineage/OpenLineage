# OpenLineage Gradle Plugins

This repository contains four Gradle plugins that are used to configure and manage the build process for OpenLineage projects. These plugins are written in Kotlin and are designed to work with Gradle.

## Scala213VariantPlugin

The `Scala213VariantPlugin` is a Gradle plugin that configures the project to use Scala 2.13. It sets up the necessary source sets, tasks, and configurations for building and testing Scala 2.13 code.

To apply this plugin, add the following to your `build.gradle.kts` (or `build.gradle`):

```kotlin
plugins {
    id("io.openlineage.scala213-variant")
}
```

## StandardSpotlessPlugin

The `StandardSpotlessPlugin` is a Gradle plugin that applies the Spotless code formatter to the project. It is configured to use the Google Java Format and to disallow wildcard imports.

To apply this plugin, add the following to your `build.gradle.kts` (or `build.gradle`):

```kotlin
plugins {
    id("io.openlineage.standard-spotless")
}
```

## CommonJavaConfigPlugin

The `CommonJavaConfigPlugin` is a Gradle plugin that applies common Java configurations to the project. It sets the source and target compatibility to Java 8 and configures the project to use Maven Central, Maven Local, and a custom Astronomer repository.

To apply this plugin, add the following to your `build.gradle.kts` (or `build.gradle`):

```kotlin
plugins {
    id("io.openlineage.common-java-config")
}
```

## PrintSourceSetConfigurationPlugin

The `PrintSourceSetConfigurationPlugin` is a Gradle plugin that registers a task named `printSourceSetConfiguration` to the project. This task, when executed, prints the configuration of the source sets in the project. This includes the name of each source set, its source directories, output directories, and compile classpath. This plugin can be useful for debugging and understanding how the source sets are configured in a project.

To apply this plugin, add the following to your `build.gradle.kts` (or `build.gradle`):

```kotlin
plugins {
    id("io.openlineage.print-source-set-configuration")
}
```

To execute the `printSourceSetConfiguration` task, run the following command in your terminal:

```bash
./gradlew printSourceSetConfiguration
```
