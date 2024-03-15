kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input1 \
  --broker-list kafka:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.json &&
kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input2 \
  --broker-list kafka:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.json &&
kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input_no_schema_registry \
  --broker-list kafka:9092 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.json &&
echo 'Events emitted'