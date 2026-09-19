# NATS Transport

This library provides a transport layer for sending emitted OpenLineage events to a [NATS](https://nats.io) subject,
through JetStream by default.

## Getting Started

### Adding the Dependency

To use this transport in your project, you need to include the following dependency in your build configuration. This is particularly important for environments like `Spark`, where this transport must be on the classpath for lineage events to be emitted correctly.

**Maven:**

```xml
<dependency>
  <groupId>io.openlineage</groupId>
  <artifactId>transports-nats</artifactId>
  <version>YOUR_VERSION_HERE</version>
</dependency>
```

The published jar relocates its `jnats` and BouncyCastle dependencies, so it does not conflict with other versions on the classpath.

#### Configuration

- `type` - string, must be `"nats"`. Required.
- `url` - NATS server URL, or several comma-separated URLs. Required.
- `subject` - subject on which events are published. Required.
- `jetstream` - publish through JetStream and wait for the stream's acknowledgement. Optional, default: `true`.
- `publishTimeout` - seconds to wait for the acknowledgement (or flush). Optional, default: `5`.
- `connectTimeout` - seconds to wait when connecting. Optional, default: `5`.
- `msgIdHeader` - set the `Nats-Msg-Id` header for JetStream de-duplication. Optional, default: `true`.
- `messageTtl` - per-message TTL in seconds; the stream must allow per-message TTL. Optional.
- `user`/`password`, `token`, `nkeysSeed` or `credsFile` - authentication, at most one method. Optional.
- `tlsKeystorePath`, `tlsKeystorePassword`, `tlsTruststorePath`, `tlsTruststorePassword` - TLS. Optional.
- `properties` - raw jnats options (`io.nats.client.*`); the settings above take precedence. Optional.

#### Behavior

- Events are serialized to JSON and published to `subject`.
- With JetStream, `emit` waits for the stream's acknowledgement and throws `OpenLineageClientException` when no stream captures the subject or the acknowledgement times out.
- `Nats-Msg-Id` is `{runId}:{eventType}:{digest}`, `job:{digest}` or `dataset:{digest}`, where the digest is a SHA-256 of the serialized event.
- With TLS, the server certificate must match the host name in `url`.
- The transport does not create streams; create one capturing the subject, with the retention (`max-age`, `max-bytes`) you need.

#### Examples

```yaml
transport:
  type: nats
  url: nats://localhost:4222
  subject: openlineage.events
  credsFile: /etc/nats/openlineage.creds
```

Spark:
```ini
spark.openlineage.transport.type=nats
spark.openlineage.transport.url=nats://localhost:4222
spark.openlineage.transport.subject=openlineage.events
spark.openlineage.transport.credsFile=/etc/nats/openlineage.creds
```

## Tests

Tests start a JetStream-enabled NATS server: `NATS_URL` when set, else the `nats-server` binary when it is on the
`PATH`, otherwise the `nats:2` Docker image through Testcontainers. They are skipped when none is available. Tests that
need a server with auth or TLS also need `nats-server` and `openssl` on the `PATH`.

`./gradlew :transports-nats:shadowJarTest` repeats the key checks against the shadow jar. `dev/nats-offline/run.sh`
runs everything in containers without internet access.
