# OpenLineage Flink

The OpenLineage Flink integration uses JVM instrumentation to emit OpenLineage metadata.
See [more docs here](https://openlineage.io/docs/integrations/flink).

## Installation

Requires running in `application mode` with setting `execution.attached: true`.
If `execution.attached` is false, we don't receive proper information about job completion.

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-flink</artifactId>
    <version>1.16.0</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-flink:0.16.0'
```

## Usage 

An instance of `OpenLineageFlinkJobListener` need to be created and registered as `jobListener`. 

This can be achieved by: 
```java
StreamExecutionEnvironment env = ...

JobListener jobListener = OpenLineageFlinkJobListener.builder()
    .executionEnvironment(env)
    .jobNamespace(jobNamespace)
    .jobName(jobName)
    .build();

env.registerJobListener(jobListener);
```

Alternatively, you can pass in job name and namespace via Flink configuration instead of specifying it in the builder

```
execution.job-listener.openlineage.job-name : "custom job name"
execution.job-listener.openlineage.namespace : "custom job namespace"
```


----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2024 contributors to the OpenLineage project