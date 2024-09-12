import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

**Tip:** See current list of [all supported transports](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports).

### [HTTP](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/HttpTransport.java)

Allows sending events to HTTP endpoint, using [ApacheHTTPClient](https://hc.apache.org/index.html).

#### Configuration

- `type` - string, must be `"http"`. Required.
- `url` - string, base url for HTTP requests. Required.
- `endpoint` - string specifying the endpoint to which events are sent, appended to `url`. Optional, default: `/api/v1/lineage`.
- `urlParams` - dictionary specifying query parameters send in HTTP requests. Optional.
- `timeoutInMillis` - integer specifying timeout (in milliseconds) value used while connecting to server. Optional, default: `5000`.
- `auth` - dictionary specifying authentication options. Optional, by default no authorization is used. If set, requires the `type` property.
  - `type` - string specifying the "api_key" or the fully qualified class name of your TokenProvider. Required if `auth` is provided.
  - `apiKey` - string setting the Authentication HTTP header as the Bearer. Required if `type` is `api_key`.
- `headers` - dictionary specifying HTTP request headers. Optional.
- `compression` - string, name of algorithm used by HTTP client to compress request body. Optional, default value `null`, allowed values: `gzip`. Added in v1.13.0.

#### Behavior

Events are serialized to JSON, and then are send as HTTP POST request with `Content-Type: application/json`.

#### Examples

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

Anonymous connection:

```yaml
transport:
  type: http
  url: http://localhost:5000
```

With authorization:

```yaml
transport:
  type: http
  url: http://localhost:5000
  auth:
    type: api_key
    api_key: f38d2189-c603-4b46-bdea-e573a3b5a7d5
```

Full example:

```yaml
transport:
  type: http
  url: http://localhost:5000
  endpoint: /api/v1/lineage
  urlParams:
    param0: value0
    param1: value1
  timeoutInMillis: 5000
  auth:
    type: api_key
    api_key: f38d2189-c603-4b46-bdea-e573a3b5a7d5
  headers:
    X-Some-Extra-Header: abc
  compression: gzip
```

</TabItem>
<TabItem value="spark" label="Spark Config">

Anonymous connection:

```ini
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://localhost:5000
```

With authorization:

```ini
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://localhost:5000
spark.openlineage.transport.auth.type=api_key
spark.openlineage.transport.auth.apiKey=f38d2189-c603-4b46-bdea-e573a3b5a7d5
```

Full example:

```ini
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://localhost:5000
spark.openlineage.transport.endpoint=/api/v1/lineage
spark.openlineage.transport.urlParams.param0=value0
spark.openlineage.transport.urlParams.param1=value1
spark.openlineage.transport.timeoutInMillis=5000
spark.openlineage.transport.auth.type=api_key
spark.openlineage.transport.auth.apiKey=f38d2189-c603-4b46-bdea-e573a3b5a7d5
spark.openlineage.transport.headers.X-Some-Extra-Header=abc
spark.openlineage.transport.compression=gzip
```

<details><summary>URL parsing within Spark integration</summary>
<p>

You can supply http parameters using values in url, the parsed `spark.openlineage.*` properties are located in url as follows:

`{transport.url}/{transport.endpoint}/namespaces/{namespace}/jobs/{parentJobName}/runs/{parentRunId}?app_name={appName}&api_key={transport.apiKey}&timeout={transport.timeout}&xxx={transport.urlParams.xxx}`

example:

`http://localhost:5000/api/v1/namespaces/ns_name/jobs/job_name/runs/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx?app_name=app&api_key=abc&timeout=5000&xxx=xxx`

</p>
</details>

</TabItem>
<TabItem value="flink" label="Flink Config">

Anonymous connection:

```ini
spark.openlineage.transport.type=http
spark.openlineage.transport.url=http://localhost:5000
```

With authorization:

```ini
openlineage.transport.type=http
openlineage.transport.url=http://localhost:5000
openlineage.transport.auth.type=api_key
openlineage.transport.auth.apiKey=f38d2189-c603-4b46-bdea-e573a3b5a7d5
```

Full example:

```ini
openlineage.transport.type=http
openlineage.transport.url=http://localhost:5000
openlineage.transport.endpoint=/api/v1/lineage
openlineage.transport.urlParams.param0=value0
openlineage.transport.urlParams.param1=value1
openlineage.transport.timeoutInMillis=5000
openlineage.transport.auth.type=api_key
openlineage.transport.auth.apiKey=f38d2189-c603-4b46-bdea-e573a3b5a7d5
openlineage.transport.headers.X-Some-Extra-Header=abc
openlineage.transport.compression=gzip
```

</TabItem>
<TabItem value="java" label="Java Code">

Anonymous connection:

```java
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;

HttpConfig httpConfig = new HttpConfig();
httpConfig.setUrl("http://localhost:5000");

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new HttpTransport(httpConfig))
  .build();
```

With authorization:

```java
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ApiKeyTokenProvider;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;

ApiKeyTokenProvider apiKeyTokenProvider = new ApiKeyTokenProvider();
apiKeyTokenProvider.setApiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5");

HttpConfig httpConfig = new HttpConfig();
httpConfig.setUrl("http://localhost:5000");
httpConfig.setAuth(apiKeyTokenProvider);

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new HttpTransport(httpConfig))
  .build();
```

Full example:

```java
import java.util.Map;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ApiKeyTokenProvider;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;

Map<String, String> queryParams = Map.of(
    "param0", "value0",
    "param1", "value1"
);

Map<String, String> headers = Map.of(
  "X-Some-Extra-Header", "abc"
);

ApiKeyTokenProvider apiKeyTokenProvider = new ApiKeyTokenProvider();
apiKeyTokenProvider.setApiKey("f38d2189-c603-4b46-bdea-e573a3b5a7d5");

HttpConfig httpConfig = new HttpConfig();
httpConfig.setUrl("http://localhost:5000");
httpConfig.setEndpoint("/api/v1/lineage");
httpConfig.setUrlParams(queryParams);
httpConfig.setAuth(apiKeyTokenProvider);
httpConfig.setTimeoutInMillis(headers);
httpConfig.setHeaders(5000);
httpConfig.setCompression(HttpConfig.Compression.GZIP);

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new HttpTransport(httpConfig))
  .build();
```

</TabItem>
</Tabs>

### [Kafka](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/KafkaTransport.java)
If a transport type is set to `kafka`, then the below parameters would be read and used when building KafkaProducer.
This transport requires the artifact `org.apache.kafka:kafka-clients:3.1.0` (or compatible) on your classpath.

#### Configuration

- `type` - string, must be `"kafka"`. Required.
- `topicName` - string specifying the topic on what events will be sent. Required.
- `properties` - a dictionary containing a Kafka producer config as in [Kafka producer config](http://kafka.apache.org/0100/documentation.html#producerconfigs). Required.
- `localServerId` - **deprecated**, renamed to `messageKey` since v1.13.0.
- `messageKey` - string, key for all Kafka messages produced by transport. Optional, default value described below. Added in v1.13.0.

  Default values for `messageKey` are:
  - `run:{parentJob.namespace}/{parentJob.name}` - for RunEvent with parent facet
  - `run:{job.namespace}/{job.name}` - for RunEvent
  - `job:{job.namespace}/{job.name}` - for JobEvent
  - `dataset:{dataset.namespace}/{dataset.name}` - for DatasetEvent

#### Behavior

Events are serialized to JSON, and then dispatched to the Kafka topic.

#### Notes

It is recommended to provide `messageKey` if Job hierarchy is used. It can be any string, but it should be the same for all jobs in
hierarchy, like `Airflow task -> Spark application -> Spark task runs`.

#### Examples

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: kafka
  topicName: openlineage.events
  properties:
    bootstrap.servers: localhost:9092,another.host:9092
    acks: all
    retries: 3
    key.serializer: org.apache.kafka.common.serialization.StringSerializer
    value.serializer: org.apache.kafka.common.serialization.StringSerializer
  messageKey: some-value
```

</TabItem>
<TabItem value="spark" label="Spark Config">

```ini
spark.openlineage.transport.type=kafka
spark.openlineage.transport.topicName=openlineage.events
spark.openlineage.transport.properties.bootstrap.servers=localhost:9092,another.host:9092
spark.openlineage.transport.properties.acks=all
spark.openlineage.transport.properties.retries=3
spark.openlineage.transport.properties.key.serializer=org.apache.kafka.common.serialization.StringSerializer
spark.openlineage.transport.properties.value.serializer=org.apache.kafka.common.serialization.StringSerializer
spark.openlineage.transport.messageKey=some-value
```

</TabItem>
<TabItem value="flink" label="Flink Config">

```ini
openlineage.transport.type=kafka
openlineage.transport.topicName=openlineage.events
openlineage.transport.properties.bootstrap.servers=localhost:9092,another.host:9092
openlineage.transport.properties.acks=all
openlineage.transport.properties.retries=3
openlineage.transport.properties.key.serializer=org.apache.kafka.common.serialization.StringSerializer
openlineage.transport.properties.value.serializer=org.apache.kafka.common.serialization.StringSerializer
openlineage.transport.messageKey=some-value
```

</TabItem>
<TabItem value="java" label="Java Code">

```java
import java.util.Properties;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.KafkaConfig;
import io.openlineage.client.transports.KafkaTransport;

Properties kafkaProperties = new Properties();
kafkaProperties.setProperty("bootstrap.servers", "localhost:9092,another.host:9092");
kafkaProperties.setProperty("acks", "all");
kafkaProperties.setProperty("retries", "3");
kafkaProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
kafkaProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

KafkaConfig kafkaConfig = new KafkaConfig();
KafkaConfig.setTopicName("openlineage.events");
KafkaConfig.setProperties(kafkaProperties);
KafkaConfig.setLocalServerId("some-value");

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new KafkaTransport(httpConfig))
  .build();
```

</TabItem>
</Tabs>

*Notes*:
It is recommended to provide `messageKey` if Job hierarchy is used. It can be any string, but it should be the same for all jobs in
hierarchy, like `Airflow task -> Spark application`.

Default values are:
- `run:{parentJob.namespace}/{parentJob.name}/{parentRun.id}` - for RunEvent with parent facet
- `run:{job.namespace}/{job.name}/{run.id}` - for RunEvent
- `job:{job.namespace}/{job.name}` - for JobEvent
- `dataset:{dataset.namespace}/{dataset.name}` - for DatasetEvent

### [Kinesis](https://github.com/OpenLineage/OpenLineage/blob/main/client/java/src/main/java/io/openlineage/client/transports/KinesisTransport.java)

If a transport type is set to `kinesis`, then the below parameters would be read and used when building KinesisProducer.
Also, KinesisTransport depends on you to provide artifact `com.amazonaws:amazon-kinesis-producer:0.14.0` or compatible on your classpath.

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

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

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

</TabItem>
<TabItem value="spark" label="Spark Config">

```ini
spark.openlineage.transport.type=kinesis
spark.openlineage.transport.streamName=your_kinesis_stream_name
spark.openlineage.transport.region=your_aws_region
spark.openlineage.transport.roleArn=arn:aws:iam::account-id:role/role-name
spark.openlineage.transport.properties.VerifyCertificate=true
spark.openlineage.transport.properties.ConnectTimeout=6000
```

</TabItem>
<TabItem value="flink" label="Flink Config">

```ini
openlineage.transport.type=kinesis
openlineage.transport.streamName=your_kinesis_stream_name
openlineage.transport.region=your_aws_region
openlineage.transport.roleArn=arn:aws:iam::account-id:role/role-name
openlineage.transport.properties.VerifyCertificate=true
openlineage.transport.properties.ConnectTimeout=6000
```

</TabItem>
<TabItem value="java" label="Java Code">

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

### [Console](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/ConsoleTransport.java)

This straightforward transport emits OpenLineage events directly to the console through a logger.
No additional configuration is required.

#### Behavior

Events are serialized to JSON. Then each event is logged with `INFO` level to logger with name `ConsoleTransport`.

#### Notes

Be cautious when using the `DEBUG` log level, as it might result in double-logging due to the `OpenLineageClient` also logging.

#### Configuration

- `type` - string, must be `"console"`. Required.

#### Examples

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: console
```

</TabItem>
<TabItem value="spark" label="Spark Config">

```ini
spark.openlineage.transport.type=console
```

</TabItem>
<TabItem value="flink" label="Flink Config">

```ini
openlineage.transport.type=console
```

</TabItem>
<TabItem value="java" label="Java Code">

```java
import java.util.Properties;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.ConsoleTransport;

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new ConsoleTransport())
  .build();
```

</TabItem>
</Tabs>

### [File](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/FileTransport.java)

Designed mainly for integration testing, the `FileTransport` emits OpenLineage events to a given file.

#### Configuration

- `type` - string, must be `"file"`. Required.
- `location` - string specifying the path of the file. Required.

#### Behavior

- If the target file is absent, it's created.
- Events are serialized to JSON, and then appended to a file, separated by newlines.
- Intrinsic newline characters within the event JSON are eliminated to ensure one-line events.

#### Notes for Yarn/Kubernetes

This transport type is pretty useless on Spark/Flink applications deployed to Yarn or Kubernetes cluster:
- Each executor will write file to a local filesystem of Yarn container/K8s pod. So resulting file will be removed when such container/pod is destroyed.
- Kubernetes persistent volumes are not destroyed after pod removal. But all the executors will write to the same network disk in parallel, producing a broken file.

#### Examples

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: file
  location: /path/to/your/file
```

</TabItem>
<TabItem value="spark" label="Spark Config">

```ini
spark.openlineage.transport.type=file
spark.openlineage.transport.location=/path/to/your/filext
```

</TabItem>
<TabItem value="flink" label="Flink Config">

```ini
openlineage.transport.type=file
openlineage.transport.location=/path/to/your/file
```

</TabItem>
<TabItem value="java" label="Java Code">

```java
import java.util.Properties;

import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.FileConfig;
import io.openlineage.client.transports.FileTransport;

FileConfig fileConfig = new FileConfig("/path/to/your/file");

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new FileTransport(fileConfig))
  .build();
```

</TabItem>
</Tabs>

## [Composite](https://github.com/OpenLineage/OpenLineage/tree/main/client/java/src/main/java/io/openlineage/client/transports/CompositeTransport.java)

The `CompositeTransport` is designed to aggregate multiple transports, allowing event emission to several destinations sequentially. This is useful when events need to be sent to multiple targets, such as a logging system and an API endpoint, one after another in a defined order.

#### Configuration

- `type` - string, must be "composite". Required.
- `transports` - may be a list or a map of transport configurations. Required.
- `continueOnFailure` - boolean flag, determines if the process should continue even when one of the transports fails. Default is `false`.

#### Behavior

- The configured transports will be initialized and used in sequence to emit OpenLineage events.
- If `continueOnFailure` is set to `false`, a failure in one transport will stop the event emission process, and an exception will be raised.
- If `continueOnFailure` is `true`, the failure will be logged, but the remaining transports will still attempt to send the event.

#### Notes for Multiple Transports
This transport can include a variety of other transport types (e.g., `HttpTransport`, `KafkaTransport`, etc.), allowing flexibility in event distribution.
Ideal for scenarios where OpenLineage events need to reach multiple destinations for redundancy or different types of processing.

The `transports` configuration can be provided in two formats:

1. A list of transport configurations, where each transport may optionally include a name field.
2. A map of transport configurations, where the key acts as the name for each transport.
The map format is particularly useful for configurations set via environment variables or Java properties, providing a more convenient and flexible setup.

#### Examples

<Tabs groupId="integrations">
<TabItem value="yaml-list" label="Yaml Config (List)">

```yaml
transport:
  type: composite
  continueOnFailure: true
  transports:
    - type: http
      url: http://example.com/api
      name: my_http
    - type: kafka
      topicName: openlineage.events
      properties:
        bootstrap.servers: localhost:9092,another.host:9092
        acks: all
        retries: 3
        key.serializer: org.apache.kafka.common.serialization.StringSerializer
        value.serializer: org.apache.kafka.common.serialization.StringSerializer
      messageKey: some-value
      continueOnFailure: true
```
</TabItem>

<TabItem value="yaml-map" label="Yaml Config (Map)">

```yaml
transport:
  type: composite
  continueOnFailure: true
  transports:
    my_http:
      type: http
      url: http://example.com/api
      name: my_http
    my_kafka:
      type: kafka
      topicName: openlineage.events
      properties:
        bootstrap.servers: localhost:9092,another.host:9092
        acks: all
        retries: 3
        key.serializer: org.apache.kafka.common.serialization.StringSerializer
        value.serializer: org.apache.kafka.common.serialization.StringSerializer
      messageKey: some-value
      continueOnFailure: true
```

</TabItem>
<TabItem value="spark" label="Spark Config">

```ini
spark.openlineage.transport.type=composite
spark.openlineage.transport.continueOnFailure=true
spark.openlineage.transport.transports.my_http.type=http
spark.openlineage.transport.transports.my_http.url=http://example.com/api
spark.openlineage.transport.transports.my_kafka.type=kafka
spark.openlineage.transport.transports.my_kafka.topicName=openlineage.events
spark.openlineage.transport.transports.my_kafka.properties.bootstrap.servers=localhost:9092,another.host:9092
spark.openlineage.transport.transports.my_kafka.properties.acks=all
spark.openlineage.transport.transports.my_kafka.properties.retries=3
spark.openlineage.transport.transports.my_kafka.properties.key.serializer=org.apache.kafka.common.serialization.StringSerializer
spark.openlineage.transport.transports.my_kafka.properties.value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

</TabItem>
<TabItem value="flink" label="Flink Config">

```ini
openlineage.transport.type=composite
openlineage.transport.continueOnFailure=true
openlineage.transport.transports.my_http.type=http
openlineage.transport.transports.my_http.url=http://example.com/api
openlineage.transport.transports.my_kafka.type=kafka
openlineage.transport.transports.my_kafka.topicName=openlineage.events
openlineage.transport.transports.my_kafka.properties.bootstrap.servers=localhost:9092,another.host:9092
openlineage.transport.transports.my_kafka.properties.acks=all
openlineage.transport.transports.my_kafka.properties.retries=3
openlineage.transport.transports.my_kafka.properties.key.serializer=org.apache.kafka.common.serialization.StringSerializer
openlineage.transport.transports.my_kafka.properties.value.serializer=org.apache.kafka.common.serialization.StringSerializer
```

</TabItem>
<TabItem value="java" label="Java Code">

```java
import java.util.Arrays;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.CompositeConfig;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import io.openlineage.client.transports.KafkaConfig;
import io.openlineage.client.transports.KafkaTransport;

HttpConfig httpConfig = new HttpConfig();
httpConfig.setUrl("http://example.com/api");
KafkaConfig kafkaConfig = new KafkaConfig();
KafkaConfig.setTopicName("openlineage.events");
KafkaConfig.setLocalServerId("some-value");

CompositeConfig compositeConfig = new CompositeConfig(Arrays.asList(
  new HttpTransport(httpConfig),
  new KafkaTransport(kafkaConfig)
), true);

OpenLineageClient client = OpenLineageClient.builder()
  .transport(
    new CompositeTransport(compositeConfig))
  .build();
```

</TabItem>
</Tabs>
