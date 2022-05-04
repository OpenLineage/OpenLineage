kafka-avro-console-producer \
  --topic io.openlineage.flink.kafka.input \
  --bootstrap-server kafka:9092 \
  --property schema.registry.url=http://schema-registry:8081 \
  --property value.schema="$(< /tmp/InputEvent.avsc)" < /tmp/events.json &&
echo 'Events emitted'