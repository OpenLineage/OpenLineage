import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


### [S3](https://github.com/OpenLineage/OpenLineage/blob/main/client/transports-s3/src/main/java/io/openlineage/client/transports/s3/S3Transport.java)

To use this transport in your project, you need to include the following dependency in your build configuration. This is
particularly important for environments like `Spark`, where this transport must be on the classpath for lineage events
to be emitted correctly.

#### Maven

```xml

<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>transports-s3</artifactId>
    <version>YOUR_VERSION_HERE</version>
</dependency>
```

#### Configuration

- `type` - string, must be `"s3"`. Required.
- `endpoint` - string, the endpoint for S3 compliant service like MinIO, Ceph, etc. Optional
- `bucketName` - string, the S3 bucket name. Required
- `fileNamePrefix` - string, prefix for the event file names. It is separated from the timestamp with underscore. It can
  include path and file name prefix. Optional.

##### Credentials

To authenticate, the transport uses
the [default credentials provider chain](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials-chain.html). The possible authentication methods include:
- Java system properties
- Environment variables
- Shared credentials config file (by default `~/.aws/config`)
- EC2 instance credentials (convenient in EMR and Glue)
- and other

Refer to the documentation for details.

#### Behavior

- Events are serialized to JSON and stored in the specified S3 bucket.
- Each event file is named based on its `eventTime`, converted to epoch milliseconds, with an optional prefix if
  configured.

#### Examples

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: s3
  endpoint: https://my-minio.example.com
  bucketName: events
  fileNamePrefix: my/service/events/event
```

</TabItem>
<TabItem value="spark" label="Spark Config">

```ini
spark.openlineage.transport.type=s3
spark.openlineage.transport.endpoint=https://my-minio.example.com
spark.openlineage.transport.bucketName=events
spark.openlineage.transport.fileNamePrefix=my/service/events/event
```

</TabItem>
<TabItem value="flink" label="Flink Config">

```ini
openlineage.transport.type=s3
openlineage.transport.endpoint=https://my-minio.example.com
openlineage.transport.bucketName=events
openlineage.transport.fileNamePrefix=my/service/events/event
```

</TabItem>
<TabItem value="java" label="Java Code">

```java
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.s3.S3TransportConfig;
import io.openlineage.client.transports.s3.S3Transport;


S3TransportConfig s3Config = new S3TransportConfig();

s3Config.setEndpoint("https://my-minio.example.com");
s3Config.setBucketName("events");
s3Config.setFileNamePrefix("my/service/events/event");

OpenLineageClient client = OpenLineageClient.builder()
        .transport(new S3Transport(s3Config))
        .build();
```

</TabItem>
</Tabs>