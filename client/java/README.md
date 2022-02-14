# OpenLineage Java Client

Java client for [OpenLineage](https://openlineage.io).

## Installation

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-java</artifactId>
    <version>0.5.2</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-java:0.5.2'
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

Use the following environment variables to configure the `HttpTransport`:

* `OPENLINEAGE_URL`: the URL for the HTTP transport (default: `http://localhost:8080`)
* `OPENLINEAGE_API_KEY`: the API key to be set on each HTTP request

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

### `YAML`

`HttpTransport` configuration:

```yaml
transport:
  type: HTTP
  url: http://localhost:5000
  apiKey: f38d2189-c603-4b46-bdea-e573a3b5a7d5
```

`KafkaTransport` configuration:

```yaml
transport:
  type: Kafka
  topicName: openlineage.events
  bootstrapServerUrl: localhost:9092
  # Kafka properties (see: http://kafka.apache.org/0100/documentation.html#producerconfigs)
  properties:
    acks: all
    retries: 3
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
```
