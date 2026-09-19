import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';


### [NATS](https://github.com/OpenLineage/OpenLineage/blob/main/client/java/transports-nats/src/main/java/io/openlineage/client/transports/nats/NatsTransport.java)

To use this transport in your project, you need to include the following dependency in your build configuration. This is
particularly important for environments like `Spark`, where this transport must be on the classpath for lineage events
to be emitted correctly.

#### Maven

```xml

<dependency>
    <groupId>io.openlineage</groupId>
    <artifactId>transports-nats</artifactId>
    <version>{{PREPROCESSOR:OPENLINEAGE_VERSION}}</version>
</dependency>
```

#### Configuration

- `type` - string, must be `"nats"`. Required.
- `url` - string, NATS server URL, or several comma-separated URLs, e.g. `nats://a:4222,nats://b:4222`. Required.
- `subject` - string, subject on which events are published. Required.
- `jetstream` - boolean, publish through JetStream and wait for the stream's acknowledgement. Optional, default: `true`.
- `publishTimeout` - number, seconds to wait for the JetStream acknowledgement (or for the flush, with `jetstream: false`). Optional, default: `5`.
- `connectTimeout` - number, seconds to wait when connecting to the server. Optional, default: `5`.
- `msgIdHeader` - boolean, set the `Nats-Msg-Id` header so that JetStream drops duplicate publishes within the stream's duplicate window. Optional, default: `true`.
- `messageTtl` - integer, per-message TTL in seconds. Requires `jetstream: true`, NATS Server 2.11+ and a stream created with per-message TTL allowed. Optional.
- `user` and `password` - strings, username/password authentication. Optional.
- `token` - string, token authentication. Optional.
- `credsFile` - string, path to a `.creds` file with a user JWT and NKey seed. Optional.
- `tlsKeystorePath`, `tlsKeystorePassword` - strings, keystore with the client certificate for TLS. Optional.
- `tlsTruststorePath`, `tlsTruststorePassword` - strings, truststore used to verify the server certificate. Optional.
- `properties` - a dictionary of [jnats options](https://github.com/nats-io/nats.java) (`io.nats.client.*`), applied before the settings above. Optional.

At most one authentication method can be configured.

#### Behavior

- Events are serialized to JSON and published to `subject`.
- With `jetstream: true`, `emit` blocks until the stream acknowledges the event and throws `OpenLineageClientException` if no stream captures the subject, or if the acknowledgement does not arrive within `publishTimeout`.
- With `jetstream: false`, events are published over core NATS. They are delivered only to subscribers connected at that moment and are lost otherwise.
- The `Nats-Msg-Id` header has the form:
  - `run:{runId}:{eventType}:{eventTime}` - for RunEvent
  - `job:{job.namespace}/{job.name}:{eventTime}` - for JobEvent
  - `dataset:{dataset.namespace}/{dataset.name}:{eventTime}` - for DatasetEvent
- The connection is opened on the first emitted event, and reopened if it was closed.

#### Stream setup

The transport does not create streams. With `jetstream: true`, a stream capturing the subject must exist before events are emitted.
Its retention settings decide how long unconsumed events are kept, for example:

```sh
nats stream add OPENLINEAGE \
  --subjects 'openlineage.>' \
  --storage file --replicas 3 \
  --retention limits \
  --max-age 7d --max-bytes 10GB --discard old \
  --dupe-window 2m
```

`--max-age` removes events that no consumer read in time, so it should be longer than the longest expected consumer downtime.

#### Examples

<Tabs groupId="integrations">
<TabItem value="yaml" label="Yaml Config">

```yaml
transport:
  type: nats
  url: nats://nats-1:4222,nats://nats-2:4222
  subject: openlineage.events
  publishTimeout: 5
  credsFile: /etc/nats/openlineage.creds
```

</TabItem>
<TabItem value="spark" label="Spark Config">

```ini
spark.openlineage.transport.type=nats
spark.openlineage.transport.url=nats://nats-1:4222,nats://nats-2:4222
spark.openlineage.transport.subject=openlineage.events
spark.openlineage.transport.publishTimeout=5
spark.openlineage.transport.credsFile=/etc/nats/openlineage.creds
```

</TabItem>
<TabItem value="flink" label="Flink Config">

```ini
openlineage.transport.type=nats
openlineage.transport.url=nats://nats-1:4222,nats://nats-2:4222
openlineage.transport.subject=openlineage.events
openlineage.transport.publishTimeout=5
openlineage.transport.credsFile=/etc/nats/openlineage.creds
```

</TabItem>
<TabItem value="java" label="Java Code">

```java
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.nats.NatsConfig;
import io.openlineage.client.transports.nats.NatsTransport;

NatsConfig natsConfig = new NatsConfig();
natsConfig.setUrl("nats://nats-1:4222,nats://nats-2:4222");
natsConfig.setSubject("openlineage.events");
natsConfig.setPublishTimeout(5.0);
natsConfig.setCredsFile("/etc/nats/openlineage.creds");

OpenLineageClient client = OpenLineageClient.builder()
  .transport(new NatsTransport(natsConfig))
  .build();
```

</TabItem>
</Tabs>
