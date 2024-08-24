---
sidebar_position: 5
---

# Java

## Overview

The OpenLineage Java is a SDK for Java programming language that users can use to generate and emit OpenLineage events to OpenLineage backends.
The core data structures currently offered by the client are the `RunEvent`, `RunState`, `Run`, `Job`, `Dataset`, 
and `Transport` classes, along with various `Facets` that can come under run, job, and dataset.

There are various [transport classes](#transports) that the library provides that carry the lineage events into various target endpoints (e.g. HTTP).

You can also use the Java client to create your own custom integrations.

## Installation

Java client is provided as library that can either be imported into your Java project using Maven or Gradle.

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-java</artifactId>
    <version>${OPENLINEAGE_VERSION}</version>
</dependency>
```

or Gradle:

```groovy
implementation("io.openlineage:openlineage-java:${OPENLINEAGE_VERSION}")
```

For more information on the available versions of the `openlineage-java`, 
please refer to the [maven repository](https://search.maven.org/artifact/io.openlineage/openlineage-java).

