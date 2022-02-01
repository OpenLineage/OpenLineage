# OpenLineage Java Client

Java client for [OpenLineage](https://openlineage.io).

## Installation

Maven:

```xml
<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>openlineage-java</artifactId>
    <version>0.5.1</version>
</dependency>
```

or Gradle:

```groovy
implementation 'io.openlineage:openlineage-java:0.5.1'
```

## Usage

```java
// Connect to http://localhost:8080
OpenLineageClient client = Clients.newClient();

// Define a simple OpenLineage START or COMPLETE event
OpenLineage.Run startOrCompleteRun = ...

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
String url = "http://localhost:5000";
String apiKey = "f38d2189-c603-4b46-bdea-e573a3b5a7d5";

OpenLineageClient client = Clients.newClient(url, apiKey);
```

To configure the client with query params appended on each HTTP request, use:

```java
URI uri = new URIBuilder("http://localhost:5000")
  .addParameter("param0", "value0")
  .addParameter("param1", "value2")
  .build();

OpenLineageClient client = Clients.newClient(uri.toURL());
```
