/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.from_unixtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Tag("integration-test")
class SparkStreamingTest {
  @Getter
  static class InputMessage {
    private final String id;
    private final long epoch;

    public InputMessage(String id, long epoch) {
      this.id = id;
      this.epoch = epoch;
    }
  }

  @Nested
  class Kafka {

    @Test
    void testKafkaSourceToKafkaSink() throws TimeoutException, StreamingQueryException {
      KafkaContainer kafka =
          new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
      kafka.start();

      createTopics(kafka.getBootstrapServers(), Arrays.asList("source-topic", "target-topic"));
      populateTopic(kafka.getBootstrapServers(), "source-topic");

      String userDirProperty = System.getProperty("user.dir");
      Path userDirPath = Paths.get(userDirProperty);
      UUID testUuid = UUID.randomUUID();
      log.info("TestUuid is {}", testUuid);

      Path derbySystemHome =
          userDirPath.resolve("tmp").resolve("derby").resolve(testUuid.toString());
      Path sparkSqlWarehouse =
          userDirPath.resolve("tmp").resolve("spark-sql-warehouse").resolve(testUuid.toString());
      Path checkpointsDir =
          userDirPath.resolve("tmp").resolve("checkpoints").resolve(testUuid.toString());

      SparkSession spark =
          SparkSession.builder()
              .appName("kafka-source-kafka-sink")
              .master("local[*]")
              .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
              .config("spark.driver.host", "localhost")
              .config("spark.driver.extraJavaOptions", "-Dderby.system.home=" + derbySystemHome)
              .config("spark.sql.warehouse.dir", sparkSqlWarehouse.toString())
              .config("spark.ui.enabled", false)
              .config("spark.openlineage.transport.type", "console")
              .config("spark.openlineage.facets.disabled", "[spark_unknown;]")
              .getOrCreate();

      Dataset<Row> sourceStream =
          spark
              .readStream()
              .format("kafka")
              .option("subscribe", "source-topic")
              .option("kafka.bootstrap.servers", kafka.getBootstrapServers())
              .option("startingOffsets", "earliest")
              .load();

      StructType schema = StructType.fromDDL("id STRING, epoch LONG");
      StreamingQuery streamingQuery =
          sourceStream
              .selectExpr("CAST(value AS STRING) AS value")
              .select(from_json(col("value"), schema).as("event"))
              .select(col("event.id").as("id"), col("event.epoch").as("epoch"))
              .select(col("id"), from_unixtime(col("epoch")).as("timestamp"))
              .select(functions.struct(col("id"), col("timestamp")).as("converted_event"))
              .select(functions.to_json(col("converted_event")).as("value"))
              .writeStream()
              .format("kafka")
              .option("topic", "target-topic")
              .option("kafka.bootstrap.servers", kafka.getBootstrapServers())
              .option("checkpointLocation", checkpointsDir.toString())
              .trigger(Trigger.ProcessingTime(Duration.ofSeconds(5).toMillis()))
              .start();

      streamingQuery.awaitTermination(Duration.ofSeconds(15).toMillis());
      spark.stop();

      kafka.stop();

      Assertions.fail("Add proper assertions to this test");
    }

    private void populateTopic(String bootstrapServers, String topic) {
      Properties props = new Properties();
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      props.put(
          ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(
          ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
      props.put(ProducerConfig.LINGER_MS_CONFIG, "100");

      ObjectMapper om = new ObjectMapper().findAndRegisterModules();
      try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
        List<Future<RecordMetadata>> futures =
            IntStream.range(0, 100)
                .mapToObj(
                    ignored ->
                        new InputMessage(
                            UUID.randomUUID().toString(), Instant.now().getEpochSecond()))
                .map(message -> serialize(om, message))
                .map(json -> new ProducerRecord<String, String>(topic, json))
                .map(
                    x ->
                        producer.send(
                            x,
                            (ignored, e) -> {
                              if (e != null) {
                                log.error("Failed to publish a message", e);
                              }
                            }))
                .collect(Collectors.toList());

        for (Future<RecordMetadata> future : futures) {
          future.get();
        }
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }

    @SneakyThrows
    private String serialize(ObjectMapper mapper, InputMessage message) {
      return mapper.writeValueAsString(message);
    }

    private void createTopics(String bootstrapServers, Collection<String> topics) {
      try (AdminClient adminClient =
          AdminClient.create(
              ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers))) {
        List<NewTopic> newTopics =
            topics.stream()
                .distinct()
                .map(topicName -> new NewTopic(topicName, 1, (short) 1))
                .collect(Collectors.toList());

        CreateTopicsResult result = adminClient.createTopics(newTopics);
        result.all().get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
