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
OpenLineageClient client = OpenLineageClient().builder()
    .baseUrl("http://localhost:5000")
    .build();

// Define a simple OpenLineage START or END event
OpenLineage.Run startOrEndRun = ...

// Emit OpenLineage event
client.emit(startOrEndRun);
```

## Configuration

|                       | **Description**                                                 |
|-----------------------|-----------------------------------------------------------------|
| `OPENLINEAGE_URL`     | The URL for the HTTP backend (default: `http://localhost:8080`) |
| `OPENLINEAGE_API_KEY` | The API key to be set on each HTTP request                      |
