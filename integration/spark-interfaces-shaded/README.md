# Spark Interfaces Shaded

`spark-interfaces-shaded` contains interfaces that can be implemented by Spark extensions to expose lineage
metadata from Spark LogicalPlan nodes which are extension's specific.

Run
```shell
./gradlew clean build  
./gradlew publishToMavenLocal
```
to build the project and publish to local Maven repository.

The contents of this package should be shaded/shadowed within the extension package to successfully expose lineage. 
This can be achieved using the `maven-shade-plugin` or Gradle’s `shadowJar` plugin. The example above shows how to do this using Maven.

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
                <pattern>io.openlineage.shaded</pattern>
                <shadedPattern>com.google.cloud.spark.bigquery.repackaged.io.openlineage</shadedPattern>
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
