# Amazon DataZone Transport

This library provides a transport layer that integrates OpenLineage with Amazon Web Service's DataZone & SageMaker Unified Studio service.

## Getting Started

### Adding the Dependency

To use this transport in your project, you need to include the following dependency in your build configuration. This is particularly important for environments like `Spark`, where this transport must be on the classpath for lineage events to be emitted correctly.


**Maven:**

```xml

<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>transports-datazone</artifactId>
    <version>YOUR_VERSION_HERE</version>
</dependency>
```

### Configuration

- `type` - string, must be `"amazon_datazone_api"`. Required.
- `domainId` - string, specifies the DataZone / SageMaker Unified Studio domain id. The lineage events will be then sent to the following domain. Required.
- `endpointOverride` - string, overrides the default HTTP endpoint for Amazon DataZone client. Optional, default: None

### Behavior

- Events are serialized to JSON, and then dispatched to the `DataZone` endpoint.

