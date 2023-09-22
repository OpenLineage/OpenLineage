# OpenLineage Java Client

Java client for [OpenLineage](https://openlineage.io).

## Installation

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-java</artifactId>
    <version>1.2.2</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-java:1.2.2'
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

To configure the client, you have several options:

- Use an `openlineage.yml` located in the user's current working directory.
- Place an `openlineage.yml` inside `.openlineage/` directory in the user's home.
    - Example: `~/.openlineage/openlineage.yml`
- Set configuration using environment variables.

> **Note:** If you don't specify configurations, the client uses default settings. However, you can easily modify these defaults.

### Example YAML Configuration

```yaml
transport:
  type: <type>
  # ... transport specific configuration
```

> **Tip:** For details on supported transport types, refer to the [`transports`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports) directory.

## Transports

The `Transport` abstraction comes with an `emit()` method tailored for the `OpenLineage.RunEvent`. Below are the available built-in transports:

### [`ConsoleTransport`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/ConsoleTransport.java)

This straightforward transport emits OpenLineage events directly to the console through a logger. Be cautious when using the DEBUG log level, as it might result in double-logging due to the `OpenLineageClient` also logging.

**Configuration**:
```yaml
transport:
  type: console
```
*No specific configuration required.*

### [`FileTransport`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/FileTransport.java)

Designed mainly for integration testing, the `FileTransport` appends OpenLineage events to a given file. Events are newline-separated, with all pre-existing newline characters within the event JSON removed.

**Configuration**:
```yaml
transport:
  type: file
  location: /path/to/your/file.txt
```

*Notes*:
- If the target file is absent, it's created.
- Events are added to the file, separated by newlines.
- Intrinsic newline characters within the event JSON are eliminated to ensure one-line events.

### [`HttpTransport`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/HttpTransport.java)

Allows configuration via environment variables to customize the URL and API key:

**YAML Configuration**:
```yaml
transport:
  type: http
  url: http://localhost:5000
  auth:
    type: api_key
    api_key: f38d2189-c603-4b46-bdea-e573a3b5a7d5
```

**Java Configuration**:
```java
OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    HttpTransport.builder()
      .url("http://localhost:5000")
      .apiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5")
      .build())
  .build();
```

For query parameters on each HTTP request:
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

Environment variables:
- `OPENLINEAGE_URL`: HTTP transport URL (default: `http://localhost:8080`).
- `OPENLINEAGE_API_KEY`: API key for each HTTP request.

*Note*: Not all features might be accessible via this method.

### [`KafkaTransport`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/KafkaTransport.java)

This transport requires the artifact `org.apache.kafka:kafka-clients:3.1.0` (or compatible) on your classpath.

**Configuration**:
```yaml
transport:
  type: kafka
  topicName: openlineage.events
  # Kafka properties (see: http://kafka.apache.org/0100/documentation.html#producerconfigs)
  properties:
    bootstrap.servers: localhost:9092,another.host:9092
    acks: all
    retries: 3
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
```

### 5. [`KinesisTransport`](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/KinesisTransport.java)

The `KinesisTransport` facilitates sending OpenLineage events to an Amazon Kinesis stream.

**Configuration**:
```yaml
transport:
  type: kinesis
  streamName: your_kinesis_stream_name
  region: your_aws_region
  roleArn: arn:aws:iam::account-id:role/role-name   # optional
  properties:  # Refer to amazon-kinesis-producer's default configuration for the available properties
    property_name_1: value_1
    property_name_2: value_2
```

**Behavior**:
- Events are serialized to JSON upon the `emit()` call and dispatched to the Kinesis stream.
- The partition key is generated by combining the job's namespace and name.
- Two constructors are available: one accepting both `KinesisProducer` and `KinesisConfig` and another solely accepting `KinesisConfig`.

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
