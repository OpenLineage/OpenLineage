kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input1 \
  --broker-list kafka-host:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.json &&
kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input2 \
  --broker-list kafka-host:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.json &&
echo 'input1 and input2 produced' &&
kafka-protobuf-console-producer \
  --topic io.openlineage.flink.kafka.protobuf_input \
  --broker-list kafka-host:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.proto)" < /tmp/events_proto.json &&
echo 'input_protobuf produced'