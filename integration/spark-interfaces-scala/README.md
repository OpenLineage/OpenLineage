# Spark Interfaces Scala

`spark-interfaces-scala` contains traits that can be implemented by Spark extensions to expose lineage
metadata from Spark LogicalPlan nodes which are extension's specific. Please refer to 
[documentation](https://openlineage.io/docs/integrations/spark/)
for more information on that. 

Run 
```shell
./gradlew clean build  
./gradlew publishToMavenLocal
```
to build the project and publish to local Maven repository. Package is built using Scala 2.13, however
it is aimed to be fully compatible with Scala 2.12 and should not use any data structures which 
are not compatible across the versions.