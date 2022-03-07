package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.transformations.SinkTransformation;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class KafkaSinkVisitor extends Visitor<Transformation<?>, OpenLineage.OutputDataset> {

    public KafkaSinkVisitor(@NonNull OpenLineageContext context) {
        super(context);
    }

    @Override
    public boolean isDefinedAt(Transformation<?> transformation) {
        return transformation instanceof SinkTransformation;
    }

    @Override
    public List<OpenLineage.OutputDataset> apply(Transformation<?> transformation) {
        if (transformation instanceof SinkTransformation) {
            KafkaSink<?> kafkaSink = (KafkaSink<?>) ((SinkTransformation<?,?,?,?>)transformation).getSink();
            Field recordSerializerField = FieldUtils.getField(KafkaSink.class, "recordSerializer", true);
            try {
                KafkaRecordSerializationSchema<?> serializationSchema = (KafkaRecordSerializationSchema<?>) recordSerializerField.get(kafkaSink);
                Field topicSelectorField = FieldUtils.getField(serializationSchema.getClass().asSubclass(serializationSchema.getClass()), "topicSelector", true);
                Field topicSelectorFunctionField = FieldUtils.getField(topicSelectorField.get(serializationSchema).getClass().asSubclass(topicSelectorField.get(serializationSchema).getClass()), "topicSelector", true);
                Function<?,?> function = (Function<?,?>) topicSelectorFunctionField.get(topicSelectorField.get(serializationSchema));
                Field kafkaProducerConfig = FieldUtils.getField(KafkaSink.class, "kafkaProducerConfig", true);
                Properties properties = (Properties) kafkaProducerConfig.get(kafkaSink);
                String bootStrapServers = properties.getProperty("bootstrap.servers");
                String kafkaTopic = (String) function.apply(null);

                log.info("Kafka output topic: {}", kafkaTopic);

                return Collections.singletonList(outputDataset().getDataset(kafkaTopic, bootStrapServers));
            } catch (IllegalAccessException e) {
                log.error("Can't access the field. ", e);
            }
        }
        return Collections.emptyList();
    }
}
