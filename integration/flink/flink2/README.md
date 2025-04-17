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