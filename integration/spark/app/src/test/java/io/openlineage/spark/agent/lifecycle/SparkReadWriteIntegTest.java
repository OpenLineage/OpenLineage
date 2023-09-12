/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static io.openlineage.client.OpenLineage.RunEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.MockBigQueryRelationProvider;
import com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Field;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Schema;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Binder;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Module;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Provides;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.SparkVersionUtils;
import io.openlineage.spark.agent.util.TestOpenLineageEventHandlerFactory;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.BinaryType$;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
@Tag("integration-test")
@Slf4j
class SparkReadWriteIntegTest {

  private static final String EVENT_TYPE = "eventType";
  private static final String NAMESPACE = "namespace";
  private static final String FILE = "file";
  private static final String NAME = "name";
  private static final String AGE = "age";
  private static final String FILE_URI_PREFIX = "file://";
  private static final String SPARK_3 = "(3.*)";
  private static final String SPARK_VERSION = "spark.version";

  private final KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.0.0"));

  @BeforeEach
  public void setUp() {
    reset(MockBigQueryRelationProvider.BIG_QUERY);
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentRunId())
        .thenReturn(Optional.of(UUID.randomUUID()));
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentJobName())
        .thenReturn("ParentJob");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getJobNamespace())
        .thenReturn("Namespace");
  }

  @AfterEach
  public void tearDown() {
    if (kafkaContainer.isCreated()) {
      kafkaContainer.stop();
    }
  }

  @Test
  void testBigQueryReadWriteToFile(@TempDir Path writeDir, SparkSession spark)
      throws InterruptedException, TimeoutException {
    TableId tableId = TableId.of("testproject", "dataset", "MyTable");
    BigQuery bq = MockBigQueryRelationProvider.BIG_QUERY;
    StructType tableSchema =
        new StructType(
            new StructField[] {
              new StructField(NAME, StringType$.MODULE$, false, Metadata.empty()),
              new StructField(AGE, LongType$.MODULE$, false, Metadata.empty())
            });

    MockBigQueryRelationProvider.INJECTOR.setTestModule(
        new Module() {
          @Override
          public void configure(Binder binder) {}

          @Provides
          public Dataset<Row> testData() {
            return spark.createDataFrame(
                Arrays.asList(
                    new GenericRowWithSchema(new Object[] {"john", 25L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"sam", 22L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"alicia", 35L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"bob", 47L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"jordan", 52L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"liz", 19L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"marcia", 83L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"maria", 40L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"luis", 8L}, tableSchema),
                    new GenericRowWithSchema(new Object[] {"gabriel", 30L}, tableSchema)),
                tableSchema);
          }
        });
    when(bq.getTable(eq(tableId)))
        .thenAnswer(
            invocation ->
                MockBigQueryRelationProvider.makeTable(
                    tableId,
                    StandardTableDefinition.newBuilder()
                        .setSchema(
                            Schema.of(
                                Field.of(NAME, StandardSQLTypeName.STRING),
                                Field.of(AGE, StandardSQLTypeName.INT64)))
                        .setNumBytes(100L)
                        .setNumRows(1000L)
                        .build()));

    Dataset<Row> df =
        spark
            .read()
            .format(MockBigQueryRelationProvider.class.getName())
            .option("gcpAccessToken", "not a real access token")
            .option("parentProject", "not a project")
            .load("testproject.dataset.MyTable");
    String outputDir = writeDir.resolve("testBigQueryRead").toAbsolutePath().toUri().getPath();
    df.write().csv(FILE_URI_PREFIX + outputDir);

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
    assertThat(events.get(2).getRun().getFacets().getAdditionalProperties())
        .hasEntrySatisfying(
            TestOpenLineageEventHandlerFactory.TEST_FACET_KEY,
            facet ->
                assertThat(facet)
                    .isInstanceOf(TestOpenLineageEventHandlerFactory.TestRunFacet.class)
                    .hasFieldOrProperty("message"));
    List<InputDataset> inputs = events.get(2).getInputs();
    assertEquals("bigquery", inputs.get(0).getNamespace());
    assertEquals(BigQueryUtil.friendlyTableName(tableId), inputs.get(0).getName());

    List<OutputDataset> outputs = events.get(2).getOutputs();
    OutputDataset output = outputs.get(0);
    assertEquals(FILE, output.getNamespace());
    assertEquals(outputDir, output.getName());
    SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI), tableSchema);
    assertThat(output.getFacets().getSchema())
        .usingRecursiveComparison()
        .isEqualTo(schemaDatasetFacet);

    assertNotNull(output.getFacets().getAdditionalProperties());
    if (SparkVersionUtils.isSpark3()) {
      assertThat(output.getOutputFacets().getOutputStatistics()).isNotNull();
    }
  }

  @Test
  void testReadFromFileWriteToJdbc(@TempDir Path writeDir, SparkSession spark)
      throws InterruptedException, TimeoutException, IOException {
    Path testFile = writeTestDataToFile(writeDir);

    Dataset<Row> df = spark.read().json(FILE_URI_PREFIX + testFile.toAbsolutePath().toString());

    Path sqliteFile = writeDir.resolve("sqlite/database");
    sqliteFile.getParent().toFile().mkdir();
    String tableName = "data_table";
    df.filter("age > 100")
        .write()
        .jdbc(
            "jdbc:sqlite:" + sqliteFile.toAbsolutePath().toUri().toString(),
            tableName,
            new Properties());
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(RunEvent.class);

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(4))
        .emit(lineageEvent.capture());
    List<RunEvent> events = lineageEvent.getAllValues();
    ObjectAssert<RunEvent> completionEvent =
        assertThat(events)
            .filteredOn(e -> e.getEventType().equals(RunEvent.EventType.COMPLETE))
            .isNotEmpty()
            .filteredOn(e -> !e.getInputs().isEmpty())
            .isNotEmpty()
            .filteredOn(e -> !e.getOutputs().isEmpty())
            .isNotEmpty()
            .filteredOn(e -> e.getOutputs().stream().anyMatch(o -> o.getOutputFacets() != null))
            .isNotEmpty()
            .first();
    completionEvent
        .extracting(RunEvent::getInputs, InstanceOfAssertFactories.list(InputDataset.class))
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE)
        .hasFieldOrPropertyWithValue(NAME, testFile.toAbsolutePath().toString());

    completionEvent
        .extracting(RunEvent::getOutputs, InstanceOfAssertFactories.list(OutputDataset.class))
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAMESPACE, "sqlite:" + sqliteFile.toAbsolutePath().toUri())
        .hasFieldOrPropertyWithValue(NAME, tableName)
        .satisfies(
            d -> {
              // Spark rowCount metrics currently only working in Spark 3.x
              if (SparkVersionUtils.isSpark3()) {
                assertThat(d.getOutputFacets().getOutputStatistics())
                    .isNotNull()
                    .hasFieldOrPropertyWithValue("rowCount", 2L);
              }
            });
  }

  private Path writeTestDataToFile(Path writeDir) throws IOException {
    writeDir.toFile().mkdirs();
    Random random = new Random();
    Path testFile = writeDir.resolve("json/testdata.json");
    testFile.getParent().toFile().mkdir();
    boolean fileCreated = testFile.toFile().createNewFile();
    if (!fileCreated) {
      throw new RuntimeException("Unable to create json input file");
    }
    log.debug("Writing test json data to {}", testFile);
    ObjectMapper mapper = new ObjectMapper();
    try (FileOutputStream writer = new FileOutputStream(testFile.toFile());
        JsonGenerator jsonWriter = mapper.getJsonFactory().createJsonGenerator(writer)) {
      for (int i = 0; i < 20; i++) {
        ImmutableMap<String, Object> map =
            ImmutableMap.of(NAME, UUID.randomUUID().toString(), AGE, random.nextInt(100));
        mapper.writeValue(jsonWriter, map);
        writer.write('\n');
      }
      mapper.writeValue(jsonWriter, ImmutableMap.of(NAME, UUID.randomUUID().toString(), AGE, 107));
      writer.write('\n');
      mapper.writeValue(jsonWriter, ImmutableMap.of(NAME, UUID.randomUUID().toString(), AGE, 103));
      writer.write('\n');
      jsonWriter.flush();
    }
    return testFile;
  }

  @Test
  void testInsertIntoDataSourceDirVisitor(@TempDir Path tempDir, SparkSession spark)
      throws IOException, InterruptedException, TimeoutException, AnalysisException {
    Path testFile = writeTestDataToFile(tempDir);
    Path parquetDir = tempDir.resolve("parquet").toAbsolutePath();
    // Two events from CreateViewCommand
    spark
        .read()
        .json(FILE_URI_PREFIX + testFile.toAbsolutePath())
        .createOrReplaceTempView("testdata");

    spark.sql(
        "INSERT OVERWRITE DIRECTORY '"
            + parquetDir
            + "'\n"
            + "USING parquet\n"
            + "SELECT * FROM testdata");
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    // The CreateView action completes quickly enough that it is sometimes missed in CI (the
    // execution id is no longer in the QueryExecution map). That makes this test sometimes flaky
    // if we expect an exact count.
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(2))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
    Optional<OpenLineage.RunEvent> completionEvent =
        events.stream()
            .filter(
                e ->
                    e.getEventType().equals(RunEvent.EventType.COMPLETE)
                        && !e.getInputs().isEmpty())
            .findFirst();
    assertTrue(completionEvent.isPresent());
    OpenLineage.RunEvent event = completionEvent.get();
    List<InputDataset> inputs = event.getInputs();
    assertEquals(1, inputs.size());
    assertEquals(FILE, inputs.get(0).getNamespace());
    assertEquals(testFile.toAbsolutePath().toString(), inputs.get(0).getName());
  }

  @Test
  void testWithExternalRdd(@TempDir Path tmpDir, SparkSession spark)
      throws InterruptedException, TimeoutException, IOException {
    Path testFile = writeTestDataToFile(tmpDir);
    JavaRDD<String> stringRdd =
        new JavaSparkContext(spark.sparkContext()).textFile(testFile.toString());
    Dataset<Row> jsonDf = spark.read().json(stringRdd);

    String outputPath = tmpDir.toAbsolutePath() + "/output_data";
    String jsonPath = FILE_URI_PREFIX + outputPath;
    jsonDf.write().json(jsonPath);
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(6))
        .emit(lineageEvent.capture());
    OpenLineage.RunEvent completeEvent = lineageEvent.getAllValues().get(5);
    assertThat(completeEvent).hasFieldOrPropertyWithValue(EVENT_TYPE, RunEvent.EventType.COMPLETE);
    assertThat(completeEvent.getInputs())
        .first()
        .hasFieldOrPropertyWithValue(NAME, testFile.getParent().toString())
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE);

    assertThat(completeEvent.getOutputs())
        .first()
        .hasFieldOrPropertyWithValue(NAME, outputPath)
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE);
  }

  @Test
  void testWithLogicalRdd(@TempDir Path tmpDir, SparkSession spark)
      throws InterruptedException, TimeoutException {
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("anInt", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("aString", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    String csvPath = tmpDir.toAbsolutePath() + "/csv_data";
    String csvUri = FILE_URI_PREFIX + csvPath;
    spark
        .createDataFrame(
            Arrays.asList(
                new GenericRow(new Object[] {1, "seven"}),
                new GenericRow(new Object[] {6, "one"}),
                new GenericRow(new Object[] {72, "fourteen"}),
                new GenericRow(new Object[] {99, "sixteen"})),
            schema)
        .write()
        .csv(csvUri);
    StaticExecutionContextFactory.waitForExecutionEnd();

    reset(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT); // reset to start counting now
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getJobNamespace())
        .thenReturn("theNamespace");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentJobName())
        .thenReturn("theParentJob");
    when(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT.getParentRunId())
        .thenReturn(Optional.of(UUID.randomUUID()));
    JobConf conf = new JobConf();
    FileInputFormat.addInputPath(conf, new org.apache.hadoop.fs.Path(csvUri));
    JavaRDD<Tuple2<LongWritable, Text>> csvRdd =
        spark
            .sparkContext()
            .hadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class, 1)
            .toJavaRDD();
    JavaRDD<Row> splitDf =
        csvRdd
            .map(t -> new String(t._2.getBytes()).split(","))
            .map(arr -> new GenericRow(new Object[] {Integer.parseInt(arr[0]), arr[1]}));
    Dataset<Row> df = spark.createDataFrame(splitDf, schema);
    String outputPath = tmpDir.toAbsolutePath() + "/output_data";
    String jsonPath = FILE_URI_PREFIX + outputPath;
    df.write().json(jsonPath);
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
        .emit(lineageEvent.capture());
    OpenLineage.RunEvent completeEvent = lineageEvent.getAllValues().get(2);
    assertThat(completeEvent).hasFieldOrPropertyWithValue(EVENT_TYPE, RunEvent.EventType.COMPLETE);
    assertThat(completeEvent.getInputs())
        .singleElement()
        .hasFieldOrPropertyWithValue(NAME, csvPath)
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE);

    assertThat(completeEvent.getOutputs())
        .singleElement()
        .hasFieldOrPropertyWithValue(NAME, outputPath)
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE);
  }

  @Test
  void testCreateDataSourceTableAsSelect(@TempDir Path tmpDir, SparkSession spark)
      throws InterruptedException, TimeoutException, IOException {
    Path testFile = writeTestDataToFile(tmpDir);
    JavaRDD<String> stringRdd =
        new JavaSparkContext(spark.sparkContext()).textFile(testFile.toString());
    Dataset<Row> jsonDf = spark.read().json(stringRdd);

    jsonDf.write().format("parquet").mode(SaveMode.Overwrite).saveAsTable("testCreateDataSource");
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(6))
        .emit(lineageEvent.capture());
    OpenLineage.RunEvent completeEvent = lineageEvent.getAllValues().get(5);
    assertThat(completeEvent).hasFieldOrPropertyWithValue(EVENT_TYPE, RunEvent.EventType.COMPLETE);
    assertThat(completeEvent.getInputs())
        .first()
        .hasFieldOrPropertyWithValue(NAME, testFile.getParent().toString())
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE);
    String warehouseDir = spark.sqlContext().conf().getConfString("spark.sql.warehouse.dir");
    assertThat(completeEvent.getOutputs())
        .first()
        .hasFieldOrPropertyWithValue(
            NAME,
            new org.apache.hadoop.fs.Path(warehouseDir).toUri().getPath() + "/testcreatedatasource")
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE)
        .extracting(OutputDataset::getFacets)
        .extracting(DatasetFacets::getSchema)
        .extracting(
            SchemaDatasetFacet::getFields,
            InstanceOfAssertFactories.list(SchemaDatasetFacetFields.class))
        .map(f -> Pair.of(f.getName(), f.getType()))
        .containsExactlyInAnyOrder(Pair.of(NAME, "string"), Pair.of(AGE, "long"));
  }

  @Test
  void testWriteWithKafkaSourceProvider(SparkSession spark)
      throws InterruptedException, TimeoutException {
    kafkaContainer.start();
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("key", StringType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("value", BinaryType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    spark
        .createDataFrame(
            Arrays.asList(
                new GenericRow(new Object[] {"seven", "seven".getBytes(StandardCharsets.UTF_8)}),
                new GenericRow(new Object[] {"one", "one".getBytes(StandardCharsets.UTF_8)}),
                new GenericRow(
                    new Object[] {"fourteen", "fourteen".getBytes(StandardCharsets.UTF_8)}),
                new GenericRow(
                    new Object[] {"sixteen", "sixteen".getBytes(StandardCharsets.UTF_8)})),
            schema)
        .write()
        .format("kafka")
        .option("topic", "topicA")
        .option("kafka.bootstrap.servers", kafkaContainer.getBootstrapServers())
        .save();

    StaticExecutionContextFactory.waitForExecutionEnd();
    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
        .emit(lineageEvent.capture());
    OpenLineage.RunEvent completeEvent = lineageEvent.getAllValues().get(2);
    assertThat(completeEvent).hasFieldOrPropertyWithValue(EVENT_TYPE, RunEvent.EventType.COMPLETE);
    String kafkaNamespace =
        "kafka://"
            + kafkaContainer.getHost()
            + ":"
            + kafkaContainer.getMappedPort(KafkaContainer.KAFKA_PORT);
    assertThat(completeEvent.getOutputs())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAME, "topicA")
        .hasFieldOrPropertyWithValue(NAMESPACE, kafkaNamespace);
  }

  @Test
  void testReadWithKafkaSourceProviderUsingAssignConfig(SparkSession spark)
      throws InterruptedException, TimeoutException, ExecutionException {
    kafkaContainer.start();
    Properties p = new Properties();
    p.setProperty("bootstrap.servers", kafkaContainer.getBootstrapServers());
    p.setProperty("key.serializer", StringSerializer.class.getName());
    p.setProperty("value.serializer", StringSerializer.class.getName());
    KafkaProducer<String, String> producer = new KafkaProducer<>(p);
    CompletableFuture.allOf(
            sendMessage(producer, new ProducerRecord<>("oneTopic", 0, "theKey", "theValue")),
            sendMessage(
                producer, new ProducerRecord<>("twoTopic", 0, "anotherKey", "anotherValue")))
        .get(10, TimeUnit.SECONDS);

    producer.flush();

    producer.close();
    Dataset<Row> kafkaDf =
        spark
            .read()
            .format("kafka")
            .option("kafka.bootstrap.servers", kafkaContainer.getBootstrapServers())
            .option("assign", "{\"oneTopic\": [0], \"twoTopic\": [0]}")
            .load();
    kafkaDf.collect();

    StaticExecutionContextFactory.waitForExecutionEnd();
    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);
    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, times(4))
        .emit(lineageEvent.capture());
    OpenLineage.RunEvent completeEvent = lineageEvent.getAllValues().get(2);
    assertThat(completeEvent).hasFieldOrPropertyWithValue(EVENT_TYPE, RunEvent.EventType.COMPLETE);
    String kafkaNamespace =
        "kafka://"
            + kafkaContainer.getHost()
            + ":"
            + kafkaContainer.getMappedPort(KafkaContainer.KAFKA_PORT);
    assertThat(completeEvent.getInputs())
        .hasSize(2)
        .satisfiesExactlyInAnyOrder(
            dataset ->
                assertThat(dataset)
                    .hasFieldOrPropertyWithValue(NAME, "oneTopic")
                    .hasFieldOrPropertyWithValue(NAMESPACE, kafkaNamespace),
            dataset -> assertThat(dataset.getName()).isEqualTo("twoTopic"));
  }

  @Test
  @EnabledIfSystemProperty(named = "spark.version", matches = "(3.*)") // Spark version >= 3.*
  void testCacheReadFromFileWriteToParquet(@TempDir Path writeDir, SparkSession spark)
      throws InterruptedException, TimeoutException, IOException {
    Path testFile = writeTestDataToFile(writeDir);
    Path tableOneDir = writeDir.resolve("table1");

    spark
        .read()
        .json(FILE_URI_PREFIX + testFile.toAbsolutePath().toString())
        .createOrReplaceTempView("raw_json");
    spark.sql("CACHE TABLE cached_json AS SELECT * FROM raw_json WHERE age > 100");
    Dataset<Row> df = spark.sql("SELECT * FROM cached_json");
    df.collect();

    Path sqliteFile = writeDir.resolve("output/database");
    sqliteFile.getParent().toFile().mkdir();
    df.write().parquet(tableOneDir.toAbsolutePath().toString());

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(1))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();

    ObjectAssert<RunEvent> completionEvent =
        assertThat(events)
            .filteredOn(e -> e.getEventType().equals(RunEvent.EventType.COMPLETE))
            .isNotEmpty()
            .filteredOn(e -> !e.getInputs().isEmpty())
            .isNotEmpty()
            .filteredOn(e -> !e.getOutputs().isEmpty())
            .isNotEmpty()
            .filteredOn(e -> e.getOutputs().stream().anyMatch(o -> o.getOutputFacets() != null))
            .isNotEmpty()
            .first();
    completionEvent
        .extracting(RunEvent::getInputs, InstanceOfAssertFactories.list(InputDataset.class))
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE)
        .hasFieldOrPropertyWithValue(NAME, testFile.toAbsolutePath().toString());

    completionEvent
        .extracting(RunEvent::getOutputs, InstanceOfAssertFactories.list(OutputDataset.class))
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE)
        .hasFieldOrPropertyWithValue(NAME, tableOneDir.toAbsolutePath().toString())
        .satisfies(
            d -> {
              // Spark rowCount metrics currently only working in Spark 3.x
              if (SparkVersionUtils.isSpark3()) {
                assertThat(d.getOutputFacets().getOutputStatistics())
                    .isNotNull()
                    .hasFieldOrPropertyWithValue("rowCount", 2L);
              }
            });
  }

  @Test
  void testSingleFileDatasets(@TempDir Path writeDir, SparkSession spark)
      throws IOException, InterruptedException, TimeoutException {
    String fileName = writeDir + "/single_file.csv";
    Files.write(Paths.get(fileName), "a,b".getBytes());

    spark.read().csv(fileName).collect();

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(1))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
    ObjectAssert<RunEvent> completionEvent =
        assertThat(events)
            .filteredOn(e -> e.getEventType().equals(RunEvent.EventType.COMPLETE))
            .isNotEmpty()
            .first();
    completionEvent
        .extracting(RunEvent::getInputs, InstanceOfAssertFactories.list(InputDataset.class))
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAMESPACE, FILE)
        .hasFieldOrPropertyWithValue(NAME, fileName);
  }

  @Test
  @EnabledIfEnvironmentVariable(named = "CI", matches = "true")
  @EnabledIfSystemProperty(named = SPARK_VERSION, matches = SPARK_3) // Spark version >= 3.*
  void testExternalRDDWithS3Bucket(SparkSession spark)
      throws InterruptedException, TimeoutException {

    spark.conf().set("fs.s3a.secret.key", System.getenv("S3_SECRET_KEY"));
    spark.conf().set("fs.s3a.access.key", System.getenv("S3_ACCESS_KEY"));

    String aDatasetName = "rdd_a_" + System.getProperty(SPARK_VERSION);
    String bDatasetName = "rdd_b_" + System.getProperty(SPARK_VERSION);
    String cDatasetName = "rdd_c_" + System.getProperty(SPARK_VERSION);

    String bucketUrl = System.getenv("S3_BUCKET");

    Dataset<Row> dataset =
        spark.createDataFrame(
            ImmutableList.of(RowFactory.create(1L, 2L), RowFactory.create(3L, 4L)),
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                  new StructField("b", LongType$.MODULE$, false, Metadata.empty())
                }));

    dataset.write().mode("overwrite").parquet(bucketUrl + "/" + aDatasetName);
    dataset.write().mode("overwrite").parquet(bucketUrl + "/" + bDatasetName);

    JavaRDD<Row> rddA = spark.read().parquet(bucketUrl + "/" + aDatasetName).toJavaRDD();
    JavaRDD<Row> rddB = spark.read().parquet(bucketUrl + "/" + bDatasetName).toJavaRDD();

    JavaRDD<Row> javaRDD =
        rddA.union(rddB).map(f -> f.getLong(0) + f.getLong(1)).map(l -> RowFactory.create(l));

    spark
        .createDataFrame(
            javaRDD,
            new StructType(
                new StructField[] {
                  new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                }))
        .toDF()
        .write()
        .mode("overwrite")
        .parquet(bucketUrl + "/" + cDatasetName);

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();
    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    Mockito.verify(SparkAgentTestExtension.OPEN_LINEAGE_SPARK_CONTEXT, atLeast(1))
        .emit(lineageEvent.capture());
    List<OpenLineage.RunEvent> events = lineageEvent.getAllValues();
    OpenLineage.RunEvent lastEvent = events.get(events.size() - 1);

    assertThat(lastEvent.getOutputs())
        .hasSize(1)
        .first()
        .hasFieldOrPropertyWithValue(NAMESPACE, bucketUrl)
        .hasFieldOrPropertyWithValue(NAME, cDatasetName);

    assertThat(lastEvent.getInputs())
        .hasSize(2)
        .first()
        .hasFieldOrPropertyWithValue(NAMESPACE, bucketUrl)
        .hasFieldOrPropertyWithValue(NAME, bDatasetName);
  }

  private CompletableFuture sendMessage(
      KafkaProducer<String, String> producer, ProducerRecord<String, String> record) {
    CompletableFuture future = new CompletableFuture();
    producer.send(
        record,
        (md, e) -> {
          if (e != null) {
            future.completeExceptionally(e);
          } else {
            future.complete(md);
          }
        });
    return future;
  }
}
