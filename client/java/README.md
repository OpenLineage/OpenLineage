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

To configure the client, use the following environment variables:

| **Environment Variable** | **Description**                                                 |
|--------------------------|-----------------------------------------------------------------|
| `OPENLINEAGE_URL`        | The URL for the HTTP backend (default: `http://localhost:8080`) |
| `OPENLINEAGE_API_KEY`    | The API key to be set on each HTTP request                      |

You can override the configuration of the client via environment variables by specifying the URL and API key when
creating a new client:

```java
OpenLineageClient client = Clients.newClient("http://localhost:5000", "f38d2189-c603-4b46-bdea-e573a3b5a7d5");
```
