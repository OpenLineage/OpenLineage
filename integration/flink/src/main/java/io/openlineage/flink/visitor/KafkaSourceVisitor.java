package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

@Slf4j
public class KafkaSourceVisitor extends Visitor<Transformation<?>, OpenLineage.InputDataset>  {

    public KafkaSourceVisitor(@NonNull OpenLineageContext context) {
        super(context);
    }

    @Override
    public boolean isDefinedAt(Transformation<?> transformation) {
        return transformation instanceof OneInputTransformation;
    }

    @Override
    public List<OpenLineage.InputDataset> apply(Transformation<?> transformation) {
        try {
            Optional<KafkaSource<?>> kafkaSource = convertIntoKafkaSource(transformation);
            if (kafkaSource.isPresent()) {
                Field subscriberField = FieldUtils.getField(KafkaSource.class, "subscriber", true);
                KafkaSubscriber kafkaSubscriber = (KafkaSubscriber) subscriberField.get(kafkaSource.get());
                Field topicsField = FieldUtils.getField(kafkaSubscriber.getClass().asSubclass(kafkaSubscriber.getClass()), "topics", true);
                List<String> topics = (List<String>) topicsField.get(kafkaSubscriber);
                Field props = FieldUtils.getField(KafkaSource.class, "props", true);
                Properties properties = (Properties) props.get(kafkaSource.get());
                String bootStrapServers = properties.getProperty("bootstrap.servers");

                topics.forEach(topic -> log.info("Kafka input topic: {}", topic));
                return topics.stream()
                        .map(topic -> inputDataset()
                        .getDataset(topic, bootStrapServers))
                        .collect(Collectors.toList());
            }
        } catch (IllegalAccessException e) {
            log.error("Can't access the field. ", e);
        }
        return Collections.emptyList();
    }

    private Optional<KafkaSource<?>> convertIntoKafkaSource(Transformation<?> transformation) {
        if (transformation instanceof OneInputTransformation) {
            return transformation.getInputs()
                    .stream()
                    .filter(PartitionTransformation.class::isInstance)
                    .findAny()
                    .map(t -> (PartitionTransformation<?>)t)
                    .map(partitionTransformation -> partitionTransformation.getInputs().get(0))
                    .filter(SourceTransformation.class::isInstance)
                    .map(source -> ((SourceTransformation<?, ?, ?>) source).getSource())
                    .map(kf -> (KafkaSource<?>) kf);
        }
        return Optional.empty();
    }
}
