# Spark Interfaces Unshaded

The `spark-interfaces-unshaded` module contains a single interface that extension developers must implement to expose 
lineage data. Given that shading of the `spark-interfaces-shaded` module is required, and the outcome will vary across 
different Spark extensions, a systematic approach is needed to "bridge" and allow the `openlineage-spark` integration to
identify the shaded resources.

Run
```shell
./gradlew clean build  
./gradlew publishToMavenLocal
```
to build the project and publish to local Maven repository.

## Purpose

This module provides a consistent way to bridge the gap between the shaded interfaces and the openlineage-spark integration.
By implementing the `io.openlineage.unshaded.spark.extension.v1.OpenLineageExtensionProvider` interface, extension 
developers can ensure that their extensions are compatible with `openlineage-spark` regardless of how the shading is performed.

## Implementation

To implement the interface:
* Include `spark-interfaces-unshaded` in your project dependencies.
* Implement the `io.openlineage.unshaded.spark.extension.v1.OpenLineageExtensionProvider` interface in your extension.
* Ensure the implementation is exposed using Java’s ServiceLoader facility by placing a configuration file in the `META-INF/services` directory within the JAR.

## Example
Here is an example of how to set up the `META-INF/services` directory:

* Create a configuration file named `io.openlineage.unshaded.spark.extension.v1.OpenLineageExtensionProvider`.
* The content of this file should be the fully qualified name of your implementation.
For example, if your implementation is `com.example.impl.MyOpenLineageExtensionProvider`, 
the file should be named `io.openlineage.unshaded.spark.extension.v1.OpenLineageExtensionProvider` and contain:
```
com.example.impl.MyOpenLineageExtensionProvider
```
This setup ensures that `openlineage-spark` can discover and use the implemented interfaces regardless of the shading performed by the extension.
