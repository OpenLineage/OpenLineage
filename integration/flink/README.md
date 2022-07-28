# OpenLineage Flink

The OpenLineage Flink uses jvm instrumentation to emit OpenLineage metadata.

## Installation

Requires running in `application mode` with setting `execution.attached: true`.
If `execution.attached` is false, we don't receive proper information about job completion.

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-flink</artifactId>
    <version>0.12.0</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-flink:0.12.0'
```

## Getting started

### Quickstart
