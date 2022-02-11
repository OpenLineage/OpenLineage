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
// Connect to http://localhost:8080
OpenLineageClient client = new OpenLineageClient();

// Define a simple OpenLineage START or COMPLETE event
OpenLineage.RunEvent startOrCompleteRun = ...

// Emit OpenLineage event
client.emit(startOrCompleteRun);
```

## Configuration

Use the following environment variables to configure the client:

* `OPENLINEAGE_URL`: the URL for the HTTP backend (default: `http://localhost:8080`)
* `OPENLINEAGE_API_KEY`: the API key to be set on each HTTP request

You can override the default configuration of the client via environment variables by specifying the URL and API key when
creating a new client:

```java
OpenLineageClient client = OpenLineageClient.builder()
  .url("http://localhost:5000")
  .apiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5")
  .build();
```

To configure the client with query params appended on each HTTP request, use:

```java
Map<String, String> queryParamsToAppend = Map.of(
  "param0","value0",
  "param1", "value1"
);

// Connect to http://localhost:5000;
OpenLineageClient client = OpenLineageClient.builder()
  .url("http://localhost:5000", queryParamsToAppend)
  .apiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5")
  .build();

// Define a simple OpenLineage START or COMPLETE event
OpenLineage.RunEvent startOrCompleteRun = ...

// Emit OpenLineage event to http://localhost:5000/api/v1/lineage?param0=value0&param1=value1
client.emit(startOrCompleteRun);
```
