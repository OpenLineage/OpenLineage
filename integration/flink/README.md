# OpenLineage Flink

The OpenLineage Flink uses JVM instrumentation to emit OpenLineage metadata.

The integration is currently very limited: [more docs here](https://openlineage.io/docs/integrations/flink).

## Installation

Requires running in `application mode` with setting `execution.attached: true`.
If `execution.attached` is false, we don't receive proper information about job completion.

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-flink</artifactId>
    <version>0.15.1</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-flink:0.15.1'
```
