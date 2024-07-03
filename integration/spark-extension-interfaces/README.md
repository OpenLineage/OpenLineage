# Spark Extension Interfaces

The `spark-extension-interfaces` module provides a set of interfaces designed to be implemented by Spark extensions.
These interfaces allow the extensions to expose lineage data in a way that is compatible with the `openlineage-spark`
integration.

## Purpose

The primary purpose of this module is to define the necessary interfaces that extensions must implement to provide
lineage data. These interfaces ensure a standardized way for extensions to communicate lineage information to
`openlineage-spark`.

## Why We Need Two Modules: `spark-extension-interfaces` and `spark-extension-frontend`

### Shading and Versioning

The `spark-extension-interfaces` module needs to be shaded (or shadowed) within each extension package to prevent
version conflicts and ensure compatibility. By shading this module, we can avoid issues arising from different versions
of the same interface being used in different extensions.

### Forward Compatibility

The `spark-extension-frontend` module contains a single interface that remains unchanged over time. This interface acts
as a bridge, allowing `openlineage-spark` to identify and interact with the shaded resources in a forward-compatible
manner. This separation ensures that even if the interfaces in `spark-extension-interfaces` evolve, the communication
protocol remains stable and compatible.

## Implementation Guide

To implement the interfaces:

* Include `spark-extension-interfaces` in your project dependencies.
* Implement the provided interfaces in your extension.
* Use the `maven-shade-plugin` or Gradle’s `shadowJar` plugin to shade the `spark-extension-interfaces` module within your
  extension package.

## Example

Here is an example configuration for shading the `spark-extension-interfaces` module using Maven:

```xml

<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <configuration>
        <skip>${shade.skip}</skip>
        <shadedArtifactAttached>false</shadedArtifactAttached>
        <relocations>
            ...
            <relocation>
                <pattern>io.openlineage.spark.shade</pattern>
                <shadedPattern>com.example.impl.repackaged.io.openlineage.spark.shade</shadedPattern>
            </relocation>
        </relocations>
        <transformers>
            <transformer
                    implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
        </transformers>
        <filters>
            <filter>
                <artifact>*:*</artifact>
                <excludes>
                    <exclude>module-info.class</exclude>
                    <exclude>META-INF/*.MF</exclude>
                    <exclude>META-INF/*.SF</exclude>
                    <exclude>META-INF/*.DSA</exclude>
                    <exclude>META-INF/*.RSA</exclude>
                    <exclude>META-INF/maven/**</exclude>
                    <exclude>META-INF/versions/**</exclude>
                    <exclude>**/*.proto</exclude>
                </excludes>
            </filter>
        </filters>
    </configuration>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
        </execution>
    </executions>
</plugin>
```
