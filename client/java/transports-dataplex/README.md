# Google Cloud Platform Dataplex Transport

This library provides a transport layer that integrates OpenLineage with Google Cloud Platform's Dataplex service.
It wraps the `com.google.cloud.datalineage:producerclient-java8` library into an OpenLineage transport, allowing you to
emit lineage events directly to Dataplex using `gRPC` channel.

## Getting Started

### Adding the Dependency

To use this transport in your project, you need to include the following dependency in your build configuration. This is
particularly important for environments like `Spark`, where this transport must be on the classpath for lineage events
to
be emitted correctly.

**Maven:**

```xml

<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>transports-dataplex</artifactId>
    <version>YOUR_VERSION_HERE</version>
</dependency>
```

### Configuration

- `type` - string, must be `"dataplex"`. Required.
- `endpoint` - string, specifies the endpoint to which events are sent. Optional.
- `projectId` - string, the project quota identifier. If not provided, it is determined based on user credentials. Optional.
- `locations` - string, [Dataplex locations](https://cloud.google.com/dataplex/docs/locations). Optional, default:
  `"us"`.
- `credentialsFile` - string, path
  to the [Service Account credentials JSON file](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account).
  Optional, if not
  provided [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
  are used

### Behavior

- Events are serialized to JSON, included as a part of `gRPC` request, and then dispatched to the `Dataplex` endpoint.
- Two constructors are available: one accepting both `SyncLineageClient` and `DataplexConfig` and another solely accepting
  `DataplexConfig`.
