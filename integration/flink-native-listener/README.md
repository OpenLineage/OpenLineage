# Flink Native Lineage Listener

Flink Native Lineage Listener package contains OpenLineage implementation of
[Flink job listener](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/advanced/job_status_listener/) to emit OpenLineage events extracted from the Flink jobs. 
It captures lineage metadata from Flink's native lineage interfaces implemented directly within Flink core and Flink connectors. 

***
Package should be considered experimental as it is built and tested on Flink version from the master branch.

This note can be removed once:
 * Lineage graph implementation becomes part of an official Flink release and this package depends on released libraries only,
 * This package gets an end-to-end test running on Docker, which will be possible once Flink's docker images is released. 
***



There exists another OpenLineage Flink integration within OpenLineage repository - `openlineage-flink`. 
While `flink-native-listener` consumes native Flink interfaces, 
the other package is using reflection to extract metadata from the Flink internals. 
The native package is also capable of extracting lineage from SQL queries while 
the other is not. Additionally, native package does not require any code changes in the job to emit OpenLineage
events. 

Flink native lineage listener will become part of `openlineage-flink.jar`.

## Usage

No code changes within existing Flink jobs are required to emit OpenLineage events.

 * Currently (October 2024), `flink-native-listener` is not included within official release, so it has to be built manually with:
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

Currently, the code depends on an unreleased Flink version and an unreleased flink-connector-kafka.
Moreover it requires specific version of those components. In general, the assumptions are:
 * `flink-connector-kafka` has to implement lineage interfaces,
 * `flink-core` has the job listener exposing LineageGraph,
 * Versions of  `flink-core` and `flink-connector-kafka` do match and can be used in a single integration test.

These assumptions are non-trivial given the fact that Flink code is undergoing huge cleanup before 
2.0 release and flink-connector-kafka is stored in a separate repository.

Given the overall above, the safest way to test OpenLineage listener is to: 

* Build Flink from sources in a version that meets the requirements above. This can be done with:

```bash
    git clone https://github.com/apache/flink
    cd flink
    git checkout 3f39a7b0822db99161cf00db23a0e160cc8684b2 # main branch commit from September 2024
    ./mvnw clean install -DskipTests
```

* Build `flink-connector-kafka`: 

```bash
    git clone https://github.com/pawel-big-lebowski/flink-connector-kafka
    cd flink-connector-kafka
    git checkout lineage-impl
    mvn clean install -DskipTests
```

This runs `flink-connector-kafka` from private fork which implements lineage source and sink interfaces.

* Run the integration tests of this package:

```bash
   ./gradlew clean build
```

## Known issues and future plans

 * Write docker integration test and depend on released Flink version (can be done after Flink release)
 * Job listener implemented within this package shall be available within standard `openlineage-flink` jar.
 * Make sure package does not contain extra dependencies (or relocates them).
 * Implement circuit breaker within the listener as in other integrations
 * Add custom Facet with Flink jobId
 * Create a tracking thread that uses Flink api to get checkpoint data. This shall be done in same way as other OpenLineage Flink integration.
 * Support schema extraction from nested Generic Records.
 * Support extracting dataset schema from Avro,
 * Support extracting dataset scheme from Protobuf.

