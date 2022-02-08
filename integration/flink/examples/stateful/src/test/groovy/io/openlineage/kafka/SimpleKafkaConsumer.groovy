package io.openlineage.kafka

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.serialization.StringDeserializer

import static java.time.Duration.ofSeconds
import static java.util.Collections.singletonList

class SimpleKafkaConsumer<T> implements AutoCloseable {
    private final KafkaConsumer<String, T> consumer

    SimpleKafkaConsumer(Kafka kafka) {
        final Properties properties = new Properties()
        properties.put('bootstrap.servers', kafka.bootstrapServers)
        properties.put('schema.registry.url', kafka.schemaRegistryUrl)
        properties.put('client.id', 'simple-kafka-consumer')
        properties.put('group.id', 'simple-kafka-consumer')
        properties.put('enable.auto.commit', 'true')
        properties.put('auto.commit.interval.ms', '1000')
        properties.put('auto.offset.reset', 'earliest')
        properties.put('key.deserializer', StringDeserializer.class)
        properties.put('value.deserializer', KafkaAvroDeserializer.class)
//        properties.put('key.deserializer', ByteArrayDeserializer.class)
//        properties.put('value.deserializer', ByteArrayDeserializer.class)
        properties.put('specific.avro.reader', true)
        properties.put('auto.register.schemas', true)
        properties.put('value.subject.name.strategy', 'io.confluent.kafka.serializers.subject.TopicNameStrategy')

        consumer = new KafkaConsumer(properties)
    }

    @Override
    void close() throws Exception {
        if (consumer != null) {
            consumer.close()
        }
    }

    void consume(String topic, int timeoutInSeconds, int numberOfRecords) throws InterruptedException {
        consumer.subscribe(singletonList(topic))
        int noRecordsCount = 0
        while (true) {
            ConsumerRecords<String, T> consumerRecords = consumer.poll(ofSeconds(timeoutInSeconds))
            if (consumerRecords.count()==0) {
                noRecordsCount++
                if (noRecordsCount > numberOfRecords)
                    break
            }
            if (consumerRecords.size() == 0) {
                println "No records found in Kafka"
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record: (%s, %d, %d, %s)\n",
                        record.key(), record.partition(), record.offset(), record.value(),)})
            consumer.commitAsync()
        }
        consumer.close()
    }

}
