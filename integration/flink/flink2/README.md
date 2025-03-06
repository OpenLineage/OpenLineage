# Flink Native Lineage Listener

Flink Native Lineage Listener package contains OpenLineage implementation of
[Flink job listener](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/advanced/job_status_listener/) to emit OpenLineage events extracted from the Flink jobs. 
It captures lineage metadata from Flink's native lineage interfaces implemented directly within Flink core and Flink connectors. 

***
Package should be considered experimental as it is built and tested on Flink 2.0-preview1 version with flink-connector-kafka
build from the feature branch. 

This note can be removed once:
 * Lineage graph implementation becomes part of an official Flink release and this package depends on released libraries only,
 * This package gets an end-to-end test running on Docker, which will be possible once Flink's docker images is released. 
***

There exists another OpenLineage Flink integration within OpenLineage repository - `openlineage-flink`, which works
for Flink versions below 2.0. While `flink-native-listener` consumes native Flink interfaces, 
the other package is using reflection to extract metadata from the Flink internals. 
The native package is also capable of extracting lineage from SQL queries while 
the other is not. Additionally, native package does not require any code changes in the job to emit OpenLineage
events. 

Flink native lineage listener will become part of `openlineage-flink.jar`.

## Usage

No code changes within existing Flink jobs are required to emit OpenLineage events.

 * Currently (December 2024), `flink-native-listener` is not included within official release, so it has to be built manually with:
```shell
./gradlew clean build
```
   Build requires Java 11.
 * Add `flink-native-listener` onto the classpath,
 * Provide standard OpenLineage configuration. See [configuration docs](https://openlineage.io/docs/client/java/configuration)

If you're new to OpenLineage, please start configuration with [transports](https://openlineage.io/docs/client/java/configuration#transports),
which define where the OpenLineage events shall be emitted.

There is an extra configuration flag relating only to `flink-native-listener`:
```yaml
openlineage.dataset.kafka.resolveTopicPattern=true
```
If set to `true`, the listener will resolve topic pattern using Kafka admin client to get information
about the topics being accessed.

## Building the jar 

Currently, the code depends on a Flink preview release and an unreleased `flink-connector-kafka`.
In order to build the jar with running tests, you need to build the `flink-connector-kafka` from the feature branch.
This will change once [FLINK-36648](https://github.com/apache/flink-connector-kafka/pull/140) is merged and released.

```bash
./buildFlinkConnectorKafka.sh
```

Run the integration tests of this package:

```bash
   ./gradlew clean build
```

## Known issues and future work

    Currently, the package should be treated experimental, used only to verify if OpenLineage events are 
    emitted correctly from the native Flink interfaces. Issues below should be implemented before
    treating the implementation as production ready.

 * Make sure package does not contain extra dependencies (or relocates them).
 * Add custom Facet with Flink jobId.
 * Create a tracking thread that uses Flink api to get checkpoint data. This shall be done in same way as other OpenLineage Flink integration.
 * Support schema extraction from nested Generic Records.
 * Support extracting dataset schema from Avro.
 * Support extracting dataset scheme from Protobuf.
 * For Kafka table, include symlink with the table name. 
 * Implement a check which will not allow turning on the listener if the Flink version is 2.
 * Prepare documentation for the package. Document some flink config entries work for Flink 2 only like - resolveTopicPattern.

