# Google Cloud Storage Transport

This library provides a transport layer for storing emitted OpenLineage events in Google Cloud Storage (GCS) buckets.

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
    <artifactId>transports-gcs</artifactId>
    <version>YOUR_VERSION_HERE</version>
</dependency>
```

### Configuration

- `type` - string, must be `"gcs"`. Required.
- `projectId` - string, the project quota identifier. Required.
- `credentialsFile` - string, path
  to the [Service Account credentials JSON file](https://developers.google.com/workspace/guides/create-credentials#create_credentials_for_a_service_account).
  Optional, if not
  provided [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
  are used
- `bucketName` - string, the GCS bucket name. Required
- `fileNamePrefix` - string, prefix for the event file names. Optional.
### Behavior

- Events are serialized to JSON and stored in the specified GCS bucket.
- Each event file is named based on its `eventTime`, converted to epoch milliseconds, with an optional prefix if configured.
- Two constructors are available: one accepting both `Storage` and `GcsTransportConfig` and another solely accepting
  `GcsTransportConfig`.
