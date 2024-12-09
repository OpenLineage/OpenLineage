/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static io.openlineage.spark.agent.SparkTestUtils.SPARK_3_OR_ABOVE;
import static io.openlineage.spark.agent.SparkTestUtils.SPARK_VERSION;
import static io.openlineage.spark.agent.SparkTestUtils.SchemaRecord;
import static io.openlineage.spark.agent.SparkTestUtils.createHttpServer;
import static io.openlineage.spark.agent.SparkTestUtils.createSparkSession;
import static io.openlineage.spark.agent.SparkTestUtils.mapToSchemaRecord;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.spark.agent.util.OpenLineageHttpHandler;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Random;
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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
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

  @Getter
  static class KafkaTestContainer {
    private final KafkaContainer kafka;
    private final String sourceTopic;
    private final String targetTopic;
    private final String bootstrapServers;

    public KafkaTestContainer(
        KafkaContainer kafka, String sourceTopic, String targetTopic, String bootstrapServers) {
      this.kafka = kafka;
      this.sourceTopic = sourceTopic;
      this.targetTopic = targetTopic;
      this.bootstrapServers = bootstrapServers;
    }

    public void stop() {
      kafka.stop();
    }

    public void close() {
      kafka.close();
    }

    public boolean isRunning() {
      return kafka.isRunning();
    }
  }

  @Getter
  static class PostgreSQLTestContainer {
    private final PostgreSQLContainer<?> postgres;

    public PostgreSQLTestContainer(PostgreSQLContainer<?> postgres) {
      this.postgres = postgres;
    }

    public void stop() {
      postgres.stop();
    }

    public String getNamespace() {
      return "postgres://" + postgres.getHost() + ":" + postgres.getMappedPort(5432).toString();
    }
  }

  private static final OpenLineageHttpHandler handler = new OpenLineageHttpHandler();

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void testKafkaSourceToKafkaSink() throws TimeoutException, StreamingQueryException, IOException {
    KafkaTestContainer kafkaContainer = setupKafkaContainer();

    String bootstrapServers = kafkaContainer.getBootstrapServers();

    UUID testUuid = UUID.randomUUID();
    log.info("TestUuid is {}", testUuid);

    HttpServer server = createHttpServer(handler);

    SparkSession spark =
        createSparkSession(server.getAddress().getPort(), "testKafkaSourceToKafkaSink");

    String userDirProperty = System.getProperty("user.dir");
    Path userDirPath = Paths.get(userDirProperty);

    Path checkpointsDir =
        userDirPath.resolve("tmp").resolve("checkpoints").resolve(testUuid.toString());

    Dataset<Row> sourceStream =
        readKafkaTopic(spark, kafkaContainer.sourceTopic, bootstrapServers)
            .transform(this::processKafkaTopic);

    StreamingQuery streamingQuery =
        sourceStream
            .writeStream()
            .format("kafka")
            .option("topic", kafkaContainer.targetTopic)
            .option("kafka.bootstrap.servers", bootstrapServers)
            .option("checkpointLocation", checkpointsDir.toString())
            .trigger(Trigger.ProcessingTime(Duration.ofSeconds(4).toMillis()))
            .start();

    streamingQuery.awaitTermination(Duration.ofSeconds(20).toMillis());

    spark.stop();

    kafkaContainer.stop();

    kafkaContainer.close();

    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> !kafkaContainer.isRunning());
    Awaitility.await().atMost(Duration.ofSeconds(60)).until(() -> spark.sparkContext().isStopped());

    List<RunEvent> events =
        handler.getEventsMap().getOrDefault("test_kafka_source_to_kafka_sink", new ArrayList<>());

    List<RunEvent> sqlEvents =
        events.stream()
            .filter(
                x -> "STREAMING".equals(x.getJob().getFacets().getJobType().getProcessingType()))
            .collect(Collectors.toList());

    assertThat(sqlEvents).isNotEmpty();

    List<RunEvent> nonEmptyInputEvents =
        events.stream().filter(x -> !x.getInputs().isEmpty()).collect(Collectors.toList());

    assertThat(nonEmptyInputEvents).isNotEmpty();

    List<SchemaRecord> expectedInputSchema =
        Arrays.asList(
            new SchemaRecord("key", "binary"),
            new SchemaRecord("value", "binary"),
            new SchemaRecord("topic", "string"),
            new SchemaRecord("partition", "integer"),
            new SchemaRecord("offset", "long"),
            new SchemaRecord("timestamp", "timestamp"),
            new SchemaRecord("timestampType", "integer"));

    List<SchemaRecord> expectedOutputSchema =
        Arrays.asList(new SchemaRecord("key", "binary"), new SchemaRecord("value", "string"));

    nonEmptyInputEvents.forEach(
        event -> {
          assertEquals(1, event.getInputs().size());
          assertEquals(kafkaContainer.sourceTopic, event.getInputs().get(0).getName());
          assertTrue(event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster:"));

          OpenLineage.SchemaDatasetFacet inputSchema =
              event.getInputs().get(0).getFacets().getSchema();

          List<SchemaRecord> inputSchemaFields = mapToSchemaRecord(inputSchema);

          assertEquals(expectedInputSchema, inputSchemaFields);

          assertEquals(1, event.getOutputs().size());
          assertEquals(kafkaContainer.targetTopic, event.getOutputs().get(0).getName());
          assertTrue(event.getOutputs().get(0).getNamespace().startsWith("kafka://prod-cluster:"));

          OpenLineage.SchemaDatasetFacet outputSchema =
              event.getOutputs().get(0).getFacets().getSchema();

          List<SchemaRecord> outputSchemaFields = mapToSchemaRecord(outputSchema);

          assertEquals(expectedOutputSchema, outputSchemaFields);
        });
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void testKafkaSourceToBatchSink() throws TimeoutException, StreamingQueryException, IOException {
    KafkaTestContainer kafkaContainer = setupKafkaContainer();

    String bootstrapServers = kafkaContainer.getBootstrapServers();

    UUID testUuid = UUID.randomUUID();
    log.info("TestUuid is {}", testUuid);

    HttpServer server = createHttpServer(handler);

    SparkSession spark =
        createSparkSession(server.getAddress().getPort(), "testKafkaSourceToBatchSink");
    spark.sparkContext().setLogLevel("ERROR");

    Dataset<Row> sourceStream =
        readKafkaTopic(spark, kafkaContainer.sourceTopic, bootstrapServers)
            .transform(this::processKafkaTopic);

    StreamingQuery streamingQuery =
        sourceStream
            .writeStream()
            .foreachBatch(
                (batch, batchId) -> {
                  batch
                      .selectExpr("CAST(key AS STRING) AS key", "CAST(value AS STRING) AS value")
                      .write()
                      .format("csv")
                      .mode("append")
                      .save("/tmp/batch_sink");
                })
            .start();

    streamingQuery.awaitTermination(Duration.ofSeconds(20).toMillis());
    List<RunEvent> events = handler.getEventsMap().get("test_kafka_source_to_batch_sink");

    assertThat(events).isNotEmpty();

    List<RunEvent> kafkaInputEvents =
        events.stream().filter(x -> !x.getInputs().isEmpty()).collect(Collectors.toList());

    assertThat(kafkaInputEvents).isNotEmpty();

    kafkaInputEvents.forEach(
        event -> {
          assertEquals(1, event.getInputs().size());
          assertEquals(kafkaContainer.sourceTopic, event.getInputs().get(0).getName());
          assertTrue(event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster"));
        });

    List<RunEvent> outputEvents =
        events.stream().filter(x -> !x.getOutputs().isEmpty()).collect(Collectors.toList());

    assertThat(outputEvents).isNotEmpty();

    outputEvents.forEach(
        event -> {
          assertEquals(1, event.getOutputs().size());
          assertEquals("/tmp/batch_sink", event.getOutputs().get(0).getName());
          assertEquals("file", event.getOutputs().get(0).getNamespace());
        });

    kafkaContainer.stop();
    spark.stop();
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void testKafkaSourceToJdbcBatchSink()
      throws TimeoutException, StreamingQueryException, IOException {
    KafkaTestContainer kafkaContainer = setupKafkaContainer();
    PostgreSQLTestContainer postgresContainer = startPostgresContainer();

    postgresContainer.postgres.getJdbcUrl();

    String bootstrapServers = kafkaContainer.getBootstrapServers();

    UUID testUuid = UUID.randomUUID();
    log.info("TestUuid is {}", testUuid);

    HttpServer server = createHttpServer(handler);

    SparkSession spark =
        createSparkSession(server.getAddress().getPort(), "testKafkaSourceToJdbcBatchSink");
    spark.sparkContext().setLogLevel("ERROR");

    Dataset<Row> sourceStream =
        readKafkaTopic(spark, kafkaContainer.sourceTopic, bootstrapServers)
            .transform(this::processKafkaTopic);

    StreamingQuery streamingQuery =
        sourceStream
            .writeStream()
            .foreachBatch(
                (batch, batchId) -> {
                  batch
                      .write()
                      .format("jdbc")
                      .option("url", postgresContainer.getPostgres().getJdbcUrl())
                      .option("driver", "org.postgresql.Driver")
                      .option("dbtable", "public.test")
                      .option("user", postgresContainer.getPostgres().getUsername())
                      .option("password", postgresContainer.getPostgres().getPassword())
                      .mode("append")
                      .save();
                })
            .start();

    streamingQuery.awaitTermination(Duration.ofSeconds(20).toMillis());

    List<RunEvent> events = handler.getEventsMap().get("test_kafka_source_to_jdbc_batch_sink");

    assertTrue(events.size() > 1);

    List<RunEvent> kafkaInputEvents =
        events.stream().filter(x -> !x.getInputs().isEmpty()).collect(Collectors.toList());

    assertThat(kafkaInputEvents).isNotEmpty();

    kafkaInputEvents.forEach(
        event -> {
          assertEquals(1, event.getInputs().size());
          assertEquals(kafkaContainer.sourceTopic, event.getInputs().get(0).getName());
          assertTrue(event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster"));
        });

    List<RunEvent> outputEvents =
        events.stream().filter(x -> !x.getOutputs().isEmpty()).collect(Collectors.toList());

    assertFalse(outputEvents.isEmpty());

    outputEvents.forEach(
        event -> {
          assertEquals(1, event.getOutputs().size());
          assertEquals("openlineage.public.test", event.getOutputs().get(0).getName());
          assertTrue(
              event.getOutputs().get(0).getNamespace().startsWith("postgres://prod-cluster"));
        });

    postgresContainer.stop();
    kafkaContainer.stop();
    spark.stop();
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void testKafkaClusterResolveNamespace()
      throws IOException, TimeoutException, StreamingQueryException {
    KafkaTestContainer kafkaContainer = setupKafkaContainer();

    HttpServer httpServer = createHttpServer(handler);

    SparkSession spark =
        createSparkSession(httpServer.getAddress().getPort(), "testKafkaClusterResolveNamespace");

    spark.sparkContext().setLogLevel("WARN");

    spark
        .readStream()
        .format("kafka")
        .option("subscribe", kafkaContainer.getSourceTopic())
        .option("kafka.bootstrap.servers", kafkaContainer.getBootstrapServers())
        .option("startingOffsets", "earliest")
        .load()
        .transform(this::processKafkaTopic)
        .writeStream()
        .format("console")
        .start()
        .awaitTermination(Duration.ofSeconds(10).toMillis());

    List<RunEvent> events =
        handler
            .getEventsMap()
            .getOrDefault("test_kafka_cluster_resolve_namespace", new ArrayList<>());

    assertTrue(events.stream().anyMatch(x -> !x.getInputs().isEmpty()));

    events.stream()
        .filter(x -> !x.getInputs().isEmpty())
        .forEach(
            event -> {
              assertEquals(1, event.getInputs().size());
              assertTrue(
                  event.getInputs().get(0).getNamespace().startsWith("kafka://prod-cluster"));
            });

    spark.stop();
    kafkaContainer.stop();
  }

  @Test
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3_OR_ABOVE)
  void readFromCsvFilesInAStreamingMode()
      throws IOException, TimeoutException, StreamingQueryException {
    HttpServer server = createHttpServer(handler);

    SparkSession spark =
        createSparkSession(server.getAddress().getPort(), "testReadFromCsvFilesInAStreamingMode");

    spark.sparkContext().setLogLevel("INFO");

    spark
        .readStream()
        .format("csv")
        .schema("name STRING, date DATE, location STRING")
        .load("src/test/resources/streaming/csvinput")
        .writeStream()
        .format("console")
        .start()
        .awaitTermination(Duration.ofSeconds(10).toMillis());

    List<RunEvent> events =
        handler.getEventsMap().get("test_read_from_csv_files_in_a_streaming_mode");

    List<RunEvent> csvInputEventsUsingStreaming =
        events.stream().filter(x -> !x.getInputs().isEmpty()).collect(Collectors.toList());

    assertFalse(csvInputEventsUsingStreaming.isEmpty());

    List<SchemaRecord> expectedSchema =
        Arrays.asList(
            new SchemaRecord("name", "string"),
            new SchemaRecord("date", "date"),
            new SchemaRecord("location", "string"));

    csvInputEventsUsingStreaming.forEach(
        event -> {
          assertEquals(1, event.getInputs().size());
          assertTrue(
              event
                  .getInputs()
                  .get(0)
                  .getName()
                  .endsWith("src/test/resources/streaming/csvinput/csv0.csv"));
          assertEquals("file", event.getInputs().get(0).getNamespace());

          OpenLineage.SchemaDatasetFacet schema = event.getInputs().get(0).getFacets().getSchema();

          List<SchemaRecord> outputSchemaFields = mapToSchemaRecord(schema);

          assertEquals(expectedSchema, outputSchemaFields);
        });

    spark.stop();
  }

  private Dataset<Row> processKafkaTopic(Dataset<Row> input) {
    StructType schema = StructType.fromDDL("id STRING, epoch LONG");
    return input
        .selectExpr("CAST(value AS STRING) AS value")
        .select(from_json(col("value"), schema).as("event"))
        .select(col("event.id").as("id"), col("event.epoch").as("epoch"))
        .select(col("id"), from_unixtime(col("epoch")).as("timestamp"))
        .select(functions.struct(col("id"), col("timestamp")).as("converted_event"))
        .select(
            expr("CAST('1' AS BINARY) AS key"),
            functions.to_json(col("converted_event")).as("value"));
  }

  private Dataset<Row> readKafkaTopic(SparkSession spark, String topic, String bootstrapServers) {
    return spark
        .readStream()
        .format("kafka")
        .option("subscribe", topic)
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("startingOffsets", "earliest")
        .load();
  }

  private PostgreSQLTestContainer startPostgresContainer() {
    PostgreSQLContainer<?> postgres =
        new PostgreSQLContainer<>(DockerImageName.parse("postgres:13"))
            .withDatabaseName("openlineage")
            .withPassword("openlineage")
            .withUsername("openlineage");

    postgres.start();

    return new PostgreSQLTestContainer(postgres);
  }

  private KafkaTestContainer setupKafkaContainer() {
    KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));
    kafka.start();

    int kafkaTopicPrefix = new Random().nextInt(1000);
    String kafkaSourceTopic = "source-topic-" + kafkaTopicPrefix;
    String kafkaTargetTopic = "target-topic-" + kafkaTopicPrefix;

    String bootstrapServers = kafka.getBootstrapServers();

    createTopics(bootstrapServers, Arrays.asList(kafkaSourceTopic, kafkaTargetTopic));
    populateTopic(bootstrapServers, kafkaSourceTopic);

    return new KafkaTestContainer(kafka, kafkaSourceTopic, kafkaTargetTopic, bootstrapServers);
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
