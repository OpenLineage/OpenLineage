package marquez.spark.agent.lifecycle;

import static marquez.spark.agent.SparkAgentTestExtension.marquezContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.BigQuery;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Field;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.MockBigQueryRelationProvider;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.Schema;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.TableId;
import com.google.cloud.spark.bigquery.repackaged.com.google.cloud.bigquery.connector.common.BigQueryUtil;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Binder;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Module;
import com.google.cloud.spark.bigquery.repackaged.com.google.inject.Provides;
import com.google.common.collect.ImmutableMap;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import marquez.spark.agent.SparkAgentTestExtension;
import marquez.spark.agent.client.LineageEvent;
import marquez.spark.agent.facets.OutputStatisticsFacet;
import marquez.spark.agent.lifecycle.plan.PlanUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import scala.Tuple2;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
public class SparkReadWriteIntegTest {

  @BeforeEach
  public void setUp() {
    reset(MockBigQueryRelationProvider.BIG_QUERY);
    when(marquezContext.getParentRunId()).thenReturn(UUID.randomUUID().toString());
    when(marquezContext.getParentJobName()).thenReturn("ParentJob");
    when(marquezContext.getJobNamespace()).thenReturn("Namespace");
  }

  @AfterEach
  public void tearDown() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @Test
  public void testBigQueryReadWriteToFile(@TempDir Path writeDir)
      throws InterruptedException, TimeoutException {
    TableId tableId = TableId.of("testproject", "dataset", "MyTable");
    BigQuery bq = MockBigQueryRelationProvider.BIG_QUERY;
    final SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Word Count")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();
    StructType tableSchema =
        new StructType(
            new StructField[] {
              new StructField("name", StringType$.MODULE$, false, null),
              new StructField("age", LongType$.MODULE$, false, null)
            });

    MockBigQueryRelationProvider.INJECTOR.setTestModule(
        new Module() {
          @Override
          public void configure(Binder binder) {}

          @Provides
          public Dataset<Row> testData() {
            return spark.createDataFrame(
                Arrays.asList(
                    new GenericRow(new Object[] {"john", 25L}),
                    new GenericRow(new Object[] {"sam", 22L}),
                    new GenericRow(new Object[] {"alicia", 35L}),
                    new GenericRow(new Object[] {"bob", 47L}),
                    new GenericRow(new Object[] {"jordan", 52L}),
                    new GenericRow(new Object[] {"liz", 19L}),
                    new GenericRow(new Object[] {"marcia", 83L}),
                    new GenericRow(new Object[] {"maria", 40L}),
                    new GenericRow(new Object[] {"luis", 8L}),
                    new GenericRow(new Object[] {"gabriel", 30L})),
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
                                Field.of("name", StandardSQLTypeName.STRING),
                                Field.of("age", StandardSQLTypeName.INT64)))
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
    df.write().csv("file://" + outputDir);

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<LineageEvent> lineageEvent = ArgumentCaptor.forClass(LineageEvent.class);
    Mockito.verify(marquezContext, times(2)).emit(lineageEvent.capture());
    List<LineageEvent> events = lineageEvent.getAllValues();
    List<LineageEvent.Dataset> inputs = events.get(1).getInputs();
    assertEquals(1, inputs.size());
    assertEquals("bigquery", inputs.get(0).getNamespace());
    assertEquals(BigQueryUtil.friendlyTableName(tableId), inputs.get(0).getName());

    List<LineageEvent.Dataset> outputs = events.get(1).getOutputs();
    assertEquals(1, outputs.size());
    LineageEvent.Dataset output = outputs.get(0);
    assertEquals("file", output.getNamespace());
    assertEquals(outputDir, output.getName());
    assertEquals(PlanUtils.schemaFacet(tableSchema), output.getFacets().getSchema());
    assertNotNull(output.getFacets().getAdditionalFacets());

    assertThat(output.getFacets().getAdditionalFacets())
        .containsKey("stats")
        .extractingByKey("stats")
        .isInstanceOf(OutputStatisticsFacet.class);
  }

  @Test
  public void testReadFromFileWriteToJdbc(@TempDir Path writeDir)
      throws InterruptedException, TimeoutException, IOException {
    Path testFile = writeTestDataToFile(writeDir);

    final SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Word Count")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();
    Dataset<Row> df = spark.read().json("file://" + testFile.toAbsolutePath().toString());

    Path sqliteFile = writeDir.resolve("sqlite/database");
    sqliteFile.getParent().toFile().mkdir();
    String tableName = "data_table";
    df.filter("age > 16")
        .write()
        .jdbc(
            "jdbc:sqlite:" + sqliteFile.toAbsolutePath().toUri().toString(),
            tableName,
            new Properties());

    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<LineageEvent> lineageEvent = ArgumentCaptor.forClass(LineageEvent.class);

    // FIXME- the DataFrame -> RDD conversion in the JDBCRelationProvider causes two different sets
    // of job execution events. Both end up triggering the open lineage event creation
    // see https://github.com/MarquezProject/marquez/issues/1197
    Mockito.verify(marquezContext, times(4)).emit(lineageEvent.capture());
    List<LineageEvent> events = lineageEvent.getAllValues();
    Optional<LineageEvent> completionEvent =
        events.stream()
            .filter(e -> e.getEventType().equals("COMPLETE") && !e.getInputs().isEmpty())
            .findFirst();
    assertTrue(completionEvent.isPresent());
    LineageEvent event = completionEvent.get();
    List<LineageEvent.Dataset> inputs = event.getInputs();
    assertEquals(1, inputs.size());
    assertEquals("file", inputs.get(0).getNamespace());
    assertEquals(testFile.toAbsolutePath().getParent().toString(), inputs.get(0).getName());

    List<LineageEvent.Dataset> outputs = event.getOutputs();
    assertEquals(1, outputs.size());
    LineageEvent.Dataset output = outputs.get(0);
    assertEquals("sqlite:" + sqliteFile.toAbsolutePath().toUri(), output.getNamespace());
    assertEquals(tableName, output.getName());
    assertNotNull(output.getFacets().getAdditionalFacets());

    assertThat(output.getFacets().getAdditionalFacets())
        .containsKey("stats")
        .extractingByKey("stats")
        .isInstanceOf(OutputStatisticsFacet.class)
        // SaveIntoDataSourceCommand doesn't accurately report stats :(
        .hasFieldOrPropertyWithValue("rowCount", 0L);
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
    ObjectMapper mapper = new ObjectMapper();
    try (FileOutputStream writer = new FileOutputStream(testFile.toFile());
        JsonGenerator jsonWriter = mapper.getJsonFactory().createJsonGenerator(writer)) {
      for (int i = 0; i < 20; i++) {
        ImmutableMap<String, Object> map =
            ImmutableMap.of("name", UUID.randomUUID().toString(), "age", random.nextInt(100));
        mapper.writeValue(jsonWriter, map);
      }
    }
    return testFile;
  }

  @Test
  public void testInsertIntoDataSourceDirVisitor(@TempDir Path tempDir)
      throws IOException, InterruptedException, TimeoutException {
    Path testFile = writeTestDataToFile(tempDir);
    final SparkSession spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("Word Count")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate();
    Path parquetDir = tempDir.resolve("parquet").toAbsolutePath();
    spark.read().json("file://" + testFile.toAbsolutePath()).createOrReplaceTempView("testdata");
    spark.sql(
        "INSERT OVERWRITE DIRECTORY '"
            + parquetDir
            + "'\n"
            + "USING parquet\n"
            + "SELECT * FROM testdata");
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<LineageEvent> lineageEvent = ArgumentCaptor.forClass(LineageEvent.class);
    Mockito.verify(marquezContext, times(4)).emit(lineageEvent.capture());
    List<LineageEvent> events = lineageEvent.getAllValues();
    Optional<LineageEvent> completionEvent =
        events.stream()
            .filter(e -> e.getEventType().equals("COMPLETE") && !e.getInputs().isEmpty())
            .findFirst();
    assertTrue(completionEvent.isPresent());
    LineageEvent event = completionEvent.get();
    List<LineageEvent.Dataset> inputs = event.getInputs();
    assertEquals(1, inputs.size());
    assertEquals("file", inputs.get(0).getNamespace());
    assertEquals(testFile.toAbsolutePath().getParent().toString(), inputs.get(0).getName());
  }

  @Test
  public void testWithLogicalRdd(@TempDir Path tmpDir)
      throws InterruptedException, TimeoutException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("anInt", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("aString", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    String csvPath = tmpDir.toAbsolutePath() + "/csv_data";
    String csvUri = "file://" + csvPath;
    session
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

    reset(marquezContext); // reset to start counting now
    when(marquezContext.getJobNamespace()).thenReturn("theNamespace");
    when(marquezContext.getParentJobName()).thenReturn("theParentJob");
    when(marquezContext.getParentRunId()).thenReturn("ABCD");
    JobConf conf = new JobConf();
    FileInputFormat.addInputPath(conf, new org.apache.hadoop.fs.Path(csvUri));
    JavaRDD<Tuple2<LongWritable, Text>> csvRdd =
        session
            .sparkContext()
            .hadoopRDD(conf, TextInputFormat.class, LongWritable.class, Text.class, 1)
            .toJavaRDD();
    JavaRDD<Row> splitDf =
        csvRdd
            .map(t -> new String(t._2.getBytes()).split(","))
            .map(arr -> new GenericRow(new Object[] {Integer.parseInt(arr[0]), arr[1]}));
    Dataset<Row> df = session.createDataFrame(splitDf, schema);
    String outputPath = tmpDir.toAbsolutePath() + "/output_data";
    String jsonPath = "file://" + outputPath;
    df.write().json(jsonPath);
    // wait for event processing to complete
    StaticExecutionContextFactory.waitForExecutionEnd();

    ArgumentCaptor<LineageEvent> lineageEvent = ArgumentCaptor.forClass(LineageEvent.class);
    Mockito.verify(marquezContext, times(2)).emit(lineageEvent.capture());
    LineageEvent completeEvent = lineageEvent.getAllValues().get(1);
    assertThat(completeEvent).hasFieldOrPropertyWithValue("eventType", "COMPLETE");
    assertThat(completeEvent.getInputs())
        .singleElement()
        .hasFieldOrPropertyWithValue("name", csvPath)
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(completeEvent.getOutputs())
        .singleElement()
        .hasFieldOrPropertyWithValue("name", outputPath)
        .hasFieldOrPropertyWithValue("namespace", "file");
  }
}
