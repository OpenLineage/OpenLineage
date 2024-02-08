# OpenLineage Gradle Plugins

This repository hosts a suite of Gradle plugins designed for configuring and managing the build
process for OpenLineage projects. Written in Kotlin, these plugins are optimized for use with
Gradle.

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

## SparkVariantBuild Plugin

### Purpose

The `SparkVariantBuild` plugin is a powerful tool designed to enhance the build process for projects that utilize Apache Spark and Scala. In environments where multiple versions of Spark and Scala are in use, ensuring compatibility and avoiding runtime failures can be challenging. The `SparkVariantBuild` plugin addresses these challenges head-on by enabling the creation of variant-specific source sets and configurations, facilitating a streamlined approach to compiling and testing code across different versions of the dependencies.

#### Key Features

- **Version-Specific Source Sets**: The plugin dynamically generates source sets for both main and test directories, named according to the pattern `mainSpark${sparkVersion}Scala${scalaBinaryVersion}` and `testSpark${sparkVersion}Scala${scalaBinaryVersion}`, where periods in version numbers are omitted. This ensures that each combination of Apache Spark and Scala versions has its compilation and testing environment, thereby improving codebase maintainability and readability.

- **Aggregate Tasks Creation**: It simplifies the build process by creating aggregate tasks for compiling source codes, running tests (both unit and integration), and packaging compiled code into JAR files. This unified approach ensures consistency across the build process and reduces the manual effort associated with managing multiple versions.

- **Utility Task for Source Set Details**: To further ease the development process, the plugin includes a utility task that prints detailed information about the created source sets. This feature provides developers with valuable insights into the configuration and setup of variant builds, aiding in troubleshooting and optimization.


### Applying the SparkVariantBuild Plugin

You can apply the plugin to your Gradle project by adding the following line to your `build.gradle.kts` script:

```kotlin
plugins {
    id("io.openlineage.spark-variant-build")
}
```

### Configuring SparkVariantBuild Plugin

After applying the plugin, you can configure it by interacting with the exposed DSL to define variant builds, their respective Apache Spark and Scala versions, and to set common dependencies, if necessary. Below is an example of how to create different Spark and Scala variant builds and share common dependencies among them:

```kotlin
builds {
    add("spark324Scala213", "3.2.4", "2.13") {
      // Configure specific build variant settings if needed.
    }

    add("spark342Scala212", "3.4.2", "2.12") {
      // Custom configurations for this variant.
    }

    commonDependencies {
      // Define dependencies common to all variants here.
      // For example, to add junit-jupiter for test implementation in all variants:
      testImmplementation(dependencies.enforcedPlatform("org.junit:junit-bom:5.10.2"))
      testImmplementation("org.junit.jupiter:junit-jupiter")
    }

    // Required, set a default build variant.
    setDefaultBuild("spark342Scala212")
}
```

In the above example, `builds` is a closure where you define multiple Spark and Scala combinations using the `add` method. Each variant can have its configurations adjusted within its block. The `commonDependencies` block is used to declare dependencies that are common across all variants, ensuring consistency and easing the management of dependencies.

By adopting this setup, you facilitate building and testing your project against multiple versions of Spark and Scala, improving the compatibility and longevity of your application.
