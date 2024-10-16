/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.listener;

import static io.openlineage.flink.testutils.KafkaTestBase.createTestTopic;
import static io.openlineage.flink.testutils.KafkaTestBase.deleteTestTopic;
import static org.apache.flink.configuration.DeploymentOptions.JOB_STATUS_CHANGED_LISTENERS;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.flink.testutils.KafkaSourceTestEnv;
import io.openlineage.flink.testutils.LineageTestUtils;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.table.KafkaDynamicTableFactory;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.SuccessException;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.junit.ClassRule;
import org.junit.FixMethodOrder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.MethodSorters;

/** Tests for job status changed listener. */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class OpenlineageListenerIntegrationTest extends TestLogger {

  private static final String TOPIC1 = "topic1";
  private static final String TOPIC2 = "topic2";
  private static final String OUTPUT_TOPIC = "output_topic";

  @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();
  private static final Path EVENTS_FILE_DIR = Paths.get("build/tests-events");
  private static final String EVENTS_FILE = EVENTS_FILE_DIR + "/" + UUID.randomUUID() + ".json";

  private static Configuration createConfiguration() {
    Configuration configuration = new Configuration();
    configuration.set(
        JOB_STATUS_CHANGED_LISTENERS,
        Collections.singletonList(OpenLineageJobStatusChangedListenerFactory.class.getName()));
    configuration.setString("openlineage.transport.type", "file");
    configuration.setString("openlineage.transport.location", EVENTS_FILE);

    // To check events with local Marquez
    // configuration.setString("openlineage.transport.type", "http");
    // configuration.setString("openlineage.transport.url", "http://localhost:9000");

    configuration.setString(
        "openlineage.dataset.kafka.resolveTopicPattern", Boolean.TRUE.toString());

    return configuration;
  }

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class KafkaSpecificTests {

    @BeforeAll
    public void setup() throws Throwable {
      KafkaSourceTestEnv.setup();
      KafkaSourceTestEnv.setupTopic(
          TOPIC1, true, true, KafkaSourceTestEnv::getRecordsForTopicWithoutTimestamp);
      KafkaSourceTestEnv.setupTopic(
          TOPIC2, true, true, KafkaSourceTestEnv::getRecordsForTopicWithoutTimestamp);
      KafkaSourceTestEnv.setupTopic(
          OUTPUT_TOPIC, true, true, KafkaSourceTestEnv::getRecordsForTopicWithoutTimestamp);
    }

    @BeforeEach
    public void beforeEach() throws Throwable {
      Files.deleteIfExists(Path.of(EVENTS_FILE));
      if (!Files.isDirectory(EVENTS_FILE_DIR)) {
        Files.createDirectory(EVENTS_FILE_DIR);
      }
    }

    @AfterAll
    void tearDown() throws Exception {
      KafkaSourceTestEnv.tearDown();
      FileUtils.deleteDirectory(EVENTS_FILE_DIR.toFile());
    }

    @org.junit.jupiter.api.Test
    @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
    void testKafkaJob() throws Exception {
      KafkaSource<PartitionAndValue> source =
          KafkaSource.<PartitionAndValue>builder()
              .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
              .setGroupId("testBasicRead")
              .setTopics(Arrays.asList(TOPIC1, TOPIC2))
              .setDeserializer(new TestingKafkaRecordDeserializationSchema(false))
              .setStartingOffsets(OffsetsInitializer.earliest())
              .setBounded(OffsetsInitializer.latest())
              .build();

      KafkaSink<PartitionAndValue> sink =
          KafkaSink.<PartitionAndValue>builder()
              .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
              .setRecordSerializer(
                  KafkaRecordSerializationSchema.builder()
                      .setValueSerializationSchema(new TestingKafkaRecordSerializationSchema())
                      .setTopic(OUTPUT_TOPIC)
                      .build())
              .build();

      try (StreamExecutionEnvironment env =
          StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration())) {

        env.setParallelism(2);
        DataStream<PartitionAndValue> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");

        stream.sinkTo(sink);
        env.execute();

        List<RunEvent> events = LineageTestUtils.fromFile(EVENTS_FILE);
        assertThat(events).isNotEmpty();

        InputDataset inputDataset = LineageTestUtils.getInputDatasets(events).get(0);
        OutputDataset outputDateset = LineageTestUtils.getOutputDatasets(events).get(0);

        assertThat(outputDateset.getNamespace()).startsWith("kafka://localhost");
        assertThat(outputDateset.getName()).startsWith(OUTPUT_TOPIC);

        assertThat(outputDateset.getFacets().getSchema().getFields()).hasSize(2);
        assertThat(outputDateset.getFacets().getSchema().getFields().get(0))
            .hasFieldOrPropertyWithValue("name", "tp")
            .hasFieldOrPropertyWithValue("type", "String");
        assertThat(outputDateset.getFacets().getSchema().getFields().get(1))
            .hasFieldOrPropertyWithValue("name", "value")
            .hasFieldOrPropertyWithValue("type", "int");

        assertThat(inputDataset.getNamespace()).startsWith("kafka://localhost");
        assertThat(inputDataset.getName()).startsWith(TOPIC1);

        assertThat(inputDataset.getFacets().getSchema().getFields()).hasSize(2);
        assertThat(inputDataset.getFacets().getSchema().getFields().get(0))
            .hasFieldOrPropertyWithValue("name", "tp")
            .hasFieldOrPropertyWithValue("type", "String");
        assertThat(inputDataset.getFacets().getSchema().getFields().get(1))
            .hasFieldOrPropertyWithValue("name", "value")
            .hasFieldOrPropertyWithValue("type", "int");
      }
    }

    @org.junit.jupiter.api.Test
    void testKafkaJobReadingTopicPattern() throws Exception {
      KafkaSource<PartitionAndValue> source =
          KafkaSource.<PartitionAndValue>builder()
              .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
              .setGroupId("testTopicPatternRead")
              .setTopicPattern(Pattern.compile("topic.*"))
              .setDeserializer(new TestingKafkaRecordDeserializationSchema(false))
              .setStartingOffsets(OffsetsInitializer.earliest())
              .setBounded(OffsetsInitializer.latest())
              .build();

      KafkaSink<PartitionAndValue> sink =
          KafkaSink.<PartitionAndValue>builder()
              .setBootstrapServers(KafkaSourceTestEnv.brokerConnectionStrings)
              .setRecordSerializer(
                  KafkaRecordSerializationSchema.builder()
                      .setValueSerializationSchema(new TestingKafkaRecordSerializationSchema())
                      .setTopic(OUTPUT_TOPIC)
                      .build())
              .build();

      try (StreamExecutionEnvironment env =
          StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration())) {

        env.setParallelism(2);
        DataStream<PartitionAndValue> stream =
            env.fromSource(source, WatermarkStrategy.noWatermarks(), "testBasicRead");

        stream.sinkTo(sink);
        env.execute();

        List<RunEvent> events = LineageTestUtils.fromFile(EVENTS_FILE);
        assertThat(LineageTestUtils.getInputDatasets(events).stream().map(d -> d.getName()))
            .containsExactly("topic1", "topic2");
      }
    }

    @org.junit.jupiter.api.Test
    @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
    void testSqlSourceSink() throws Exception {
      StreamExecutionEnvironment env =
          StreamExecutionEnvironment.getExecutionEnvironment(createConfiguration());
      env.setParallelism(1);
      StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

      // we always use a different topic name for each parameterized topic,
      // in order to make sure the topic can be created.
      final String inputTopic = "tstopic_" + "_" + UUID.randomUUID();
      final String outputTopic = "tstopic_" + "_" + UUID.randomUUID();
      createTestTopic(inputTopic, 1, 1);

      // ---------- Produce an event time stream into Kafka -------------------
      String groupId = "testSql";
      String bootstraps = KafkaSourceTestEnv.brokerConnectionStrings;

      final String createInputTable =
          String.format(
              "create table kafka_input (\n"
                  + "  `computed-price` as price + 1.0,\n"
                  + "  price decimal(38, 18),\n"
                  + "  currency string,\n"
                  + "  log_date date,\n"
                  + "  log_time time(3),\n"
                  + "  log_ts timestamp(3),\n"
                  + "  ts as log_ts + INTERVAL '1' SECOND,\n"
                  + "  watermark for ts as ts\n"
                  + ") with (\n"
                  + "  'connector' = '%s',\n"
                  + "  'topic' = '%s',\n"
                  + "  'properties.bootstrap.servers' = '%s',\n"
                  + "  'properties.group.id' = '%s',\n"
                  + "  'scan.startup.mode' = 'earliest-offset',\n"
                  + "  'format' = 'json'\n"
                  + ")",
              KafkaDynamicTableFactory.IDENTIFIER, inputTopic, bootstraps, groupId);
      tEnv.executeSql(createInputTable);

      final String createOutputTable =
          String.format(
              "create table kafka_output (\n"
                  + "  ts_interval string,\n"
                  + "  max_log_date string,\n"
                  + "  max_log_time string,\n"
                  + "  max_ts string,\n"
                  + "  counter bigint,\n"
                  + "  max_price decimal(38, 18)\n"
                  + ") with (\n"
                  + "  'connector' = '%s',\n"
                  + "  'topic' = '%s',\n"
                  + "  'properties.bootstrap.servers' = '%s',\n"
                  + "  'properties.group.id' = '%s',\n"
                  + "  'scan.startup.mode' = 'earliest-offset',\n"
                  + "  'format' = 'json'\n"
                  + ")",
              KafkaDynamicTableFactory.IDENTIFIER, outputTopic, bootstraps, groupId);
      tEnv.executeSql(createOutputTable);

      String initialValues =
          "INSERT INTO kafka_input\n"
              + "SELECT CAST(price AS DECIMAL(10, 2)), currency, "
              + " CAST(d AS DATE), CAST(t AS TIME(0)), CAST(ts AS TIMESTAMP(3))\n"
              + "FROM (VALUES (2.02,'Euro','2019-12-12', '00:00:01', '2019-12-12 00:00:01.001001'), \n"
              + "  (1.11,'US Dollar','2019-12-12', '00:00:02', '2019-12-12 00:00:02.002001'), \n"
              + "  (50,'Yen','2019-12-12', '00:00:03', '2019-12-12 00:00:03.004001'), \n"
              + "  (3.1,'Euro','2019-12-12', '00:00:04', '2019-12-12 00:00:04.005001'), \n"
              + "  (5.33,'US Dollar','2019-12-12', '00:00:05', '2019-12-12 00:00:05.006001'), \n"
              + "  (0,'DUMMY','2019-12-12', '00:00:10', '2019-12-12 00:00:10'))\n"
              + "  AS orders (price, currency, d, t, ts)";
      tEnv.executeSql(initialValues).await();

      // ---------- Consume stream from Kafka -------------------

      String query =
          "INSERT INTO kafka_output SELECT\n"
              + "  CAST(TUMBLE_END(ts, INTERVAL '5' SECOND) AS VARCHAR),\n"
              + "  CAST(MAX(log_date) AS VARCHAR),\n"
              + "  CAST(MAX(log_time) AS VARCHAR),\n"
              + "  CAST(MAX(ts) AS VARCHAR),\n"
              + "  COUNT(*),\n"
              + "  CAST(MAX(price) AS DECIMAL(10, 2))\n"
              + "FROM kafka_input\n"
              + "GROUP BY TUMBLE(ts, INTERVAL '5' SECOND)";

      try {
        tEnv.executeSql(query).await(6, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        // ok
      }

      List<RunEvent> events =
          Files.readAllLines(Path.of(EVENTS_FILE)).stream()
              .map(OpenLineageClientUtils::runEventFromJson)
              .collect(Collectors.toList());

      Optional<InputDataset> input =
          events.stream()
              .filter(e -> e.getInputs() != null)
              .filter(e -> e.getInputs().size() > 0)
              .map(e -> e.getInputs().get(0))
              .findAny();
      assertThat(input).isPresent();
      assertThat(input.get().getNamespace()).startsWith("kafka://localhost:");
      assertThat(input.get().getName()).isEqualTo(inputTopic);
      assertThat(input.get().getFacets().getSymlinks().getIdentifiers().get(0))
          .hasFieldOrPropertyWithValue("type", "TABLE")
          .hasFieldOrPropertyWithValue("name", "default_catalog.default_database.kafka_input");
      assertThat(input.get().getFacets().getSymlinks().getIdentifiers().get(0).getNamespace())
          .startsWith("kafka://localhost:");
      List<SchemaDatasetFacetFields> inputFields = input.get().getFacets().getSchema().getFields();
      assertThat(inputFields).hasSize(5);
      assertFieldPresent(inputFields, "price", "DECIMAL(38, 18)");
      assertFieldPresent(inputFields, "currency", "STRING");
      assertFieldPresent(inputFields, "log_date", "DATE");
      assertFieldPresent(inputFields, "log_time", "TIME(0)");
      assertFieldPresent(inputFields, "log_ts", "TIMESTAMP(3)");

      assertThat(
              events.stream()
                  .filter(e -> e.getOutputs() != null)
                  .filter(e -> e.getOutputs().size() > 0)
                  .map(e -> e.getOutputs().get(0))
                  .findAny())
          .isPresent();

      Optional<OutputDataset> output =
          events.stream()
              .filter(e -> e.getOutputs() != null)
              .filter(e -> e.getOutputs().size() > 0)
              .map(e -> e.getOutputs().get(0))
              .findAny();
      assertThat(output.get().getNamespace()).startsWith("kafka://localhost:");
      assertThat(output.get().getName()).isEqualTo(outputTopic);
      List<SchemaDatasetFacetFields> outputFields =
          output.get().getFacets().getSchema().getFields();
      assertFieldPresent(outputFields, "ts_interval", "STRING");
      assertFieldPresent(outputFields, "max_log_date", "STRING");
      assertFieldPresent(outputFields, "max_log_time", "STRING");
      assertFieldPresent(outputFields, "max_ts", "STRING");
      assertFieldPresent(outputFields, "counter", "BIGINT");
      assertFieldPresent(outputFields, "max_price", "DECIMAL(38, 18)");

      assertThat(output.get().getFacets().getSymlinks().getIdentifiers().get(0))
          .hasFieldOrPropertyWithValue("type", "TABLE")
          .hasFieldOrPropertyWithValue("name", "default_catalog.default_database.kafka_output");
      assertThat(output.get().getFacets().getSymlinks().getIdentifiers().get(0).getNamespace())
          .startsWith("kafka://localhost:");

      // ------------- cleanup -------------------
      deleteTestTopic(inputTopic);
      deleteTestTopic(outputTopic);
    }
  }

  /**
   * Asserts field of a given name and type exists among the given fields' list
   *
   * @param fields
   * @param name
   * @param type
   */
  private void assertFieldPresent(List<SchemaDatasetFacetFields> fields, String name, String type) {
    assertThat(fields.stream().filter(f -> f.getName().startsWith(name)).findAny())
        .isPresent()
        .map(f -> f.getType())
        .hasValue(type);
  }

  private static class PartitionAndValue implements Serializable {
    private static final long serialVersionUID = 4813439951036021779L;
    public String tp;
    public int value;

    public PartitionAndValue() {}

    private PartitionAndValue(TopicPartition tp, int value) {
      this.tp = tp.toString();
      this.value = value;
    }
  }

  private static class TestingKafkaRecordDeserializationSchema
      implements KafkaRecordDeserializationSchema<PartitionAndValue> {
    private static final long serialVersionUID = -3765473065594331694L;
    private transient Deserializer<Integer> deserializer;
    private final boolean enableObjectReuse;
    private final PartitionAndValue reuse = new PartitionAndValue();

    public TestingKafkaRecordDeserializationSchema(boolean enableObjectReuse) {
      this.enableObjectReuse = enableObjectReuse;
    }

    @Override
    public void deserialize(
        ConsumerRecord<byte[], byte[]> record, Collector<PartitionAndValue> collector)
        throws IOException {
      if (deserializer == null) {
        deserializer = new IntegerDeserializer();
      }

      if (enableObjectReuse) {
        reuse.tp = new TopicPartition(record.topic(), record.partition()).toString();
        reuse.value = deserializer.deserialize(record.topic(), record.value());
        collector.collect(reuse);
      } else {
        collector.collect(
            new PartitionAndValue(
                new TopicPartition(record.topic(), record.partition()),
                deserializer.deserialize(record.topic(), record.value())));
      }
    }

    @Override
    public TypeInformation<PartitionAndValue> getProducedType() {
      return TypeInformation.of(PartitionAndValue.class);
    }
  }

  private static class TestingKafkaRecordSerializationSchema
      implements SerializationSchema<PartitionAndValue> {
    private static final long serialVersionUID = -3765473065594331694L;

    @Override
    public byte[] serialize(PartitionAndValue partitionAndValue) {
      return BigInteger.valueOf(partitionAndValue.value).toByteArray();
    }
  }

  private static final class TestingSinkFunction implements SinkFunction<RowData> {

    private static final long serialVersionUID = 455430015321124493L;
    private static List<String> rows = new ArrayList<>();

    private final int expectedSize;

    private TestingSinkFunction(int expectedSize) {
      this.expectedSize = expectedSize;
      rows.clear();
    }

    @Override
    public void invoke(RowData value, Context context) {
      rows.add(value.toString());
      if (rows.size() >= expectedSize) {
        // job finish
        throw new SuccessException();
      }
    }
  }
}
