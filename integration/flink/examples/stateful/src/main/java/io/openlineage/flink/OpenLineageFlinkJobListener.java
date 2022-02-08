package io.openlineage.flink;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class OpenLineageFlinkJobListener implements JobListener {

    private final StreamExecutionEnvironment executionEnvironment;
    private static final Logger LOGGER = LoggerFactory.getLogger(OpenLineageFlinkJobListener.class);


    OpenLineageFlinkJobListener(StreamExecutionEnvironment executionEnvironment) {
        this.executionEnvironment = executionEnvironment;
        try {
            Field transformations = FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
            ArrayList<Transformation<?>> previousTransformationList = (ArrayList<Transformation<?>>) FieldUtils.readField(transformations, executionEnvironment, true);
            List<Transformation<?>> transformationList = new UnclearableList<>(previousTransformationList);
            FieldUtils.writeField(transformations, executionEnvironment, transformationList, true);
        } catch (IllegalAccessException e) {
            LOGGER.error("Failed to rewrite transforamtions");
        }
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        if (jobClient != null) {
            LOGGER.info("JobId: {}", jobClient.getJobID());
            Field transformationsField = FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
            try {
                List<Transformation> transformations = (List<Transformation>) transformationsField.get(executionEnvironment);
                transformations.forEach(transformation -> {
                    if (transformation instanceof OneInputTransformation) {
                        extractKafkaSourceTopics(transformation);
                    } else if (transformation instanceof SinkTransformation) {
                        extractKafkaSinkTopics(transformation);
                    }
                });
            } catch (IllegalAccessException e) {
                LOGGER.error("Can't access the field. ", e);
            }
        }
    }

    private void extractKafkaSourceTopics(Transformation transformation) {
        transformation.getInputs().stream()
                .map(partitionTransformation -> ((PartitionTransformation) partitionTransformation).getInputs())
                .forEach(inputs -> ((List<SourceTransformation>) inputs).forEach(sourceTransformation -> {
                            KafkaSource kafkaSource = (KafkaSource) ((SourceTransformation) sourceTransformation).getSource();
                            Field subscriberField = FieldUtils.getField(KafkaSource.class, "subscriber", true);
                            try {
                                KafkaSubscriber kafkaSubscriber = (KafkaSubscriber) subscriberField.get(kafkaSource);
                                Field topicsField = FieldUtils.getField(kafkaSubscriber.getClass().asSubclass(kafkaSubscriber.getClass()), "topics", true);
                                List<String> topics = (List<String>) topicsField.get(kafkaSubscriber);
                                topics.forEach(topic -> LOGGER.info("Kafka input topic: {}", topic));
                            } catch (IllegalAccessException e) {
                                LOGGER.error("Can't access the field. ", e);
                            }
                        })
                );
    }

    private void extractKafkaSinkTopics(Transformation transformation) {
        KafkaSink kafkaSink = (KafkaSink) ((SinkTransformation)transformation).getSink();
        Field recordSerializerField = FieldUtils.getField(KafkaSink.class, "recordSerializer", true);
        try {
            KafkaRecordSerializationSchema serializationSchema = (KafkaRecordSerializationSchema) recordSerializerField.get(kafkaSink);
            Field topicSelectorField = FieldUtils.getField(serializationSchema.getClass().asSubclass(serializationSchema.getClass()), "topicSelector", true);
            Field topicSelectorFunctionField = FieldUtils.getField(topicSelectorField.get(serializationSchema).getClass().asSubclass(topicSelectorField.get(serializationSchema).getClass()), "topicSelector", true);
            Function function = (Function) topicSelectorFunctionField.get(topicSelectorField.get(serializationSchema));
            LOGGER.error("Kafka output topic: {}", function.apply(null));
        } catch (IllegalAccessException e) {
            LOGGER.error("Can't access the field. ", e);
        }
    }



    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        LOGGER.info("JobExecutionResult: {}", jobExecutionResult);
    }

    static class UnclearableList<T> extends ArrayList<T> {

        public UnclearableList(Collection<T> collection) {
            super(collection);
            LOGGER.warn("Created unclearableList");
        }

        @Override
        public void clear() {
            LOGGER.info("Trying to clear unclearable list");
        }
    }
}