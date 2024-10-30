# AWS Kinesis Transport

This library provides a transport layer for sending emitted OpenLineage events to AWS Kinesis streams.

## Getting Started

### Adding the Dependency

To use this transport in your project, you need to include the following dependency in your build configuration. This is particularly important for environments like `Spark`, where this transport must be on the classpath for lineage events to be emitted correctly.

**Maven:**

```xml
<dependency>
  <groupId>io.openlineage</groupId>
  <artifactId>transports-kinesis</artifactId>
  <version>YOUR_VERSION_HERE</version>
</dependency>
```

#### Configuration

- `type` - string, must be `"kinesis"`. Required.
- `streamName` - the streamName of the Kinesis. Required.
- `region` - the region of the Kinesis. Required.
- `roleArn` - the roleArn which is allowed to read/write to Kinesis stream. Optional.
- `properties` - a dictionary that contains a [Kinesis allowed properties](https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties). Optional.

#### Behavior

- Events are serialized to JSON, and then dispatched to the Kinesis stream.
- The partition key is generated as `{jobNamespace}:{jobName}`.
- Two constructors are available: one accepting both `KinesisProducer` and `KinesisConfig` and another solely accepting `KinesisConfig`.

#### Examples

```yaml
transport:
  type: kinesis
  streamName: your_kinesis_stream_name
  region: your_aws_region
  roleArn: arn:aws:iam::account-id:role/role-name
  properties:
    VerifyCertificate: true
    ConnectTimeout: 6000
```


Spark:
```ini
spark.openlineage.transport.type=kinesis
spark.openlineage.transport.streamName=your_kinesis_stream_name
spark.openlineage.transport.region=your_aws_region
spark.openlineage.transport.roleArn=arn:aws:iam::account-id:role/role-name
spark.openlineage.transport.properties.VerifyCertificate=true
spark.openlineage.transport.properties.ConnectTimeout=6000
```

Flink:
```ini
openlineage.transport.type=kinesis
openlineage.transport.streamName=your_kinesis_stream_name
openlineage.transport.region=your_aws_region
openlineage.transport.roleArn=arn:aws:iam::account-id:role/role-name
openlineage.transport.properties.VerifyCertificate=true
openlineage.transport.properties.ConnectTimeout=6000
```

Code:
```java
import java.util.Properties;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.KinesisConfig;
import io.openlineage.client.transports.KinesisTransport;

Properties kinesisProperties = new Properties();
kinesisProperties.setProperty("property_name_1", "value_1");
kinesisProperties.setProperty("property_name_2", "value_2");

KinesisConfig kinesisConfig = new KinesisConfig();
kinesisConfig.setStreamName("your_kinesis_stream_name");
kinesisConfig.setRegion("your_aws_region");
kinesisConfig.setRoleArn("arn:aws:iam::account-id:role/role-name");
kinesisConfig.setProperties(kinesisProperties);

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new KinesisTransport(httpConfig))
  .build();
```

</TabItem>
</Tabs>
