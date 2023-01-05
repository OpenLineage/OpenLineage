# OpenLineage - Flink sample stateful application

Flink stateful application for local environment, which includes Kafka (zookeeper and broker), SchemaRegistry and Flink cluster (JobManager and TaskManager).
There is a kafka container [kafka-topic-setup](src/docker/docker-compose.yml) used to create kafka topics that are required by the sample application.
Flink application is running in [Application Mode](https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/overview/#deployment-modes) which requires jars to be available on the classpath.
For simplicity, cluster uses only one broker and zookeeper, one SchemaRegistry.

Flink Stateful application simply takes event [InputEvent](src/main/avro/io/openlineage/flink/avro/event/InputEvent.avsc) from Kafka topic (`io.openlineage.flink.kafka.input`), counts number of occurrences based on unique `id`, 
and sends out the [OutputEvent](src/main/avro/io/openlineage/flink/avro/event/OutputEvent.avsc) to different kafka topic (`io.openlineage.flink.kafka.output`).

## Pre-requisites

1. Docker
2. Java 8+

## Getting started

### Quickstart
To be able to start the cluster flink-stataful-application has to be build:
```
./gradle build
```
which will create `build/lib` folder with complete artifact (`*.jar`).

This artifact will be mounted to the Flink (`/opt/flink/usrlib`).
Next, start the docker container (from the folder where docker-compose is [located](src/docker/docker-compose.yml):
```bash
docker-compose up -d
```
`-d` - detached mode

This will start Flink cluster on [localhost:18081](http://localhost:18081/). Flink application will be up and running.
Stateful flink application will run with state in memory.
Kafka BootstrapServers on `'127.0.0.1:19092'` and SchemaRegistry on` 'http://0.0.0.0:28081'`

```bash
> docker-compose ps

      Name                     Command               State                      Ports
--------------------------------------------------------------------------------------------------------
jobmanager          /docker-entrypoint.sh stan ...   Up       6123/tcp, 0.0.0.0:18081->8081/tcp
kafka               /etc/confluent/docker/run        Up       0.0.0.0:19092->19092/tcp, 9092/tcp
schema-registry     /etc/confluent/docker/run        Up       0.0.0.0:28081->28081/tcp, 8081/tcp
taskmanager         /docker-entrypoint.sh task ...   Up       6123/tcp, 8081/tcp
zookeeper           /etc/confluent/docker/run        Up       0.0.0.0:2181->2181/tcp, 2888/tcp, 3888/tcp
kafka-topic-setup   /bin/sh -c                       Exit 0

```
### Testing

To be able to test if the Flink application is able to process events we have to sent events to the Input topic, and we can expect events in output topic.
For this purpose, there is [Sandbox](src/test/groovy/io/openlineage/sandbox/Sandbox.groovy) created with useful commands to try-out.

----
SPDX-License-Identifier: Apache-2.0\
Copyright 2018-2023 contributors to the OpenLineage project