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

 * Make sure package does not contain extra dependencies (or relocates them).
 * Add custom Facet with Flink jobId.
 * Support schema extraction from nested Generic Records.
 * Support extracting dataset schema from Avro.
 * Support extracting dataset scheme from Protobuf.
 * For Kafka table, include symlink with the table name. 
 * Implement a check which will not allow turning on the listener if the Flink version is 2.
 * Prepare documentation for the package. Document some flink config entries work for Flink 2 only like - resolveTopicPattern, trackingIntervalInSeconds

