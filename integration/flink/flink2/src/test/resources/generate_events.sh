#!/bin/bash
# Copyright 2018-2025 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0

set -e

# Produce Avro messages to input1
kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input1 \
  --broker-list kafka-host:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.jsonl &&
  
# Produce Avro messages to input2
kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input2 \
  --broker-list kafka-host:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.jsonl &&
echo 'input1 and input2 produced'

## Produce Protobuf messages
#kafka-protobuf-console-producer \
#  --topic io.openlineage.flink.kafka.protobuf_input \
#  --broker-list kafka-host:9092 \
#  --property schema.registry.url=http://schema-registry:8081 \
#  --property value.schema="$(< /tmp/InputEvent.proto)" < /tmp/events_proto.jsonl &&
#echo 'input_protobuf produced'