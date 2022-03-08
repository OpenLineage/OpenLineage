package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;

@Slf4j
public class KafkaSourceVisitor extends Visitor<OpenLineage.InputDataset> {

  public KafkaSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object source) {
    return source instanceof KafkaSource;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object kafkaSource) {
    var source = (KafkaSource<?>) kafkaSource;
    try {
      Field subscriberField = FieldUtils.getField(KafkaSource.class, "subscriber", true);
      KafkaSubscriber kafkaSubscriber = (KafkaSubscriber) subscriberField.get(source);
      Field topicsField =
          FieldUtils.getField(
              kafkaSubscriber.getClass().asSubclass(kafkaSubscriber.getClass()), "topics", true);
      List<String> topics = (List<String>) topicsField.get(kafkaSubscriber);
      Field props = FieldUtils.getField(KafkaSource.class, "props", true);
      Properties properties = (Properties) props.get(source);
      String bootStrapServers = properties.getProperty("bootstrap.servers");

      topics.forEach(topic -> log.debug("Kafka input topic: {}", topic));
      return topics.stream()
          .map(topic -> inputDataset().getDataset(topic, bootStrapServers))
          .collect(Collectors.toList());
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
    return Collections.emptyList();
  }
}
