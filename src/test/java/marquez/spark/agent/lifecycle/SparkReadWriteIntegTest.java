package marquez.spark.agent.lifecycle;

import static marquez.spark.agent.SparkAgentTestExtension.marquezContext;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
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
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import marquez.spark.agent.SparkAgentTestExtension;
import marquez.spark.agent.client.LineageEvent;
import marquez.spark.agent.facets.OutputStatisticsFacet;
import marquez.spark.agent.lifecycle.plan.PlanUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

@ExtendWith(SparkAgentTestExtension.class)
public class SparkReadWriteIntegTest {

  @BeforeEach
  public void setUp() {
    Mockito.reset(MockBigQueryRelationProvider.BIG_QUERY);
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

    when(marquezContext.getParentRunId()).thenReturn(UUID.randomUUID().toString());
    when(marquezContext.getJobName()).thenReturn("ParentJob");
    when(marquezContext.getJobNamespace()).thenReturn("Namespace");

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

    assertThat(output.getFacets().getAdditionalFacets(), hasKey("stats"));
    assertThat(
        output.getFacets().getAdditionalFacets().get("stats"),
        instanceOf(OutputStatisticsFacet.class));
  }
}
