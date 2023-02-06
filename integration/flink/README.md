# OpenLineage Flink

The OpenLineage Flink integration uses JVM instrumentation to emit OpenLineage metadata.

The integration is currently very limited. See [more docs here](https://openlineage.io/docs/integrations/flink).

## Installation

Requires running in `application mode` with setting `execution.attached: true`.
If `execution.attached` is false, we don't receive proper information about job completion.

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-flink</artifactId>
    <version>0.20.0</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-flink:0.16.0'
```

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project