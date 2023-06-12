# OpenLineage Java Client

Java client for [OpenLineage](https://openlineage.io).

## Installation

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-java</artifactId>
    <version>0.28.0</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-java:0.28.0'
```

## Usage

```java
// Use openlineage.yml
OpenLineageClient client = Clients.newClient();

// Define a simple OpenLineage START or COMPLETE event
OpenLineage.RunEvent startOrCompleteRun = ...

// Emit OpenLineage event
client.emit(startOrCompleteRun);
```

## Configuration

Use the following options to configure the client:

* An `openlineage.yml` in the user's current working directory
* An `openlineage.yml` under `.openlineage/` in the user's home directory (ex: `~/.openlineage/openlineage.yml`)
* Environment variables

> **Note:** By default, the client will give you sane defaults, but you can easily override them.
>

`YAML`

```yaml
transport:
  type: <type>
  # ... transport specific configuration
```

> **Note:** For a full list of supported transports, see [`transports`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports).

## Transports

The `Transport` abstraction defines an `emit()` method for   `OpenLineage.RunEvent`. There are three built-in transports: `ConsoleTransport`, `HttpTransport`, and `KafkaTransport`.

### [`HttpTransport`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/HttpTransport.java)

```yaml
transport:
  type: HTTP
  url: http://localhost:5000
  auth:
    type: api_key
    api_key: f38d2189-c603-4b46-bdea-e573a3b5a7d5
```

You can override the default configuration of the `HttpTransport` via environment variables by specifying the URL and API key when
creating a new client:

```java
OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    HttpTransport.builder()
      .url("http://localhost:5000")
      .apiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5")
      .build())
  .build();
```

To configure the client with query params appended on each HTTP request, use:

```java
Map<String, String> queryParamsToAppend = Map.of(
  "param0","value0",
  "param1", "value1"
);

// Connect to http://localhost:5000
OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    HttpTransport.builder()
      .url("http://localhost:5000", queryParamsToAppend)
      .apiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5")
      .build())
  .build();

// Define a simple OpenLineage START or COMPLETE event
OpenLineage.RunEvent startOrCompleteRun = ...

// Emit OpenLineage event to http://localhost:5000/api/v1/lineage?param0=value0&param1=value1
client.emit(startOrCompleteRun);
```

Alternatively, use the following environment variables to configure the `HttpTransport`:

* `OPENLINEAGE_URL`: the URL for the HTTP transport (default: `http://localhost:8080`)
* `OPENLINEAGE_API_KEY`: the API key to be set on each HTTP request

Not everything will be supported while using this method.

### [`KafkaTransport`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/KafkaTransport.java)

```yaml
transport:
  type: Kafka
  topicName: openlineage.events
  # Kafka properties (see: http://kafka.apache.org/0100/documentation.html#producerconfigs)
  properties:
    bootstrap.servers: localhost:9092,another.host:9092
    acks: all
    retries: 3
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
```

KafkaTransport depends on you to provide artifact `org.apache.kafka:kafka-clients:3.1.0` or compatible on your classpath.

## Facets Configuration

You can specify a list of disabled facets that will not be included in OpenLineage event. 

```yaml
facets:
  disabled: 
    - spark_unknown
    - spark_logicalPlan
```

## Error Handling

```java
// Connect to http://localhost:5000
OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    HttpTransport.builder()
      .url("http://localhost:5000")
      .apiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5")
      .build())
  .registerErrorHandler(new EmitErrorHandler() {
    @Override
    public void handleError(Throwable throwable) {
      // Handle emit error here
    }
  }).build();
```

## Defining Your Own Transport

```java
OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new MyTransport() {
      @Override
      public void emit(OpenLineage.RunEvent runEvent) {
        // Add emit logic here
      }
    }).build();
```

## Contributing

If contributing changes, additions or fixes to the Java client, please include the following header in any new `.java` files:

```
/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0 
*/
```

The Github Actions Super-linter is installed and configured to check for headers in new `.java` files when pull requests are opened.

Thanks for your contributions to the project!

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project