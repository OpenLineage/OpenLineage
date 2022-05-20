package io.openlineage.spark3.agent.lifecycle.plan.columnLineage;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.LastQueryExecutionSparkEventListener;
import java.util.Optional;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashMap;

@Slf4j
public class ColumnLevelLineageUtilsNonV2CatalogTest {

  SparkSession spark;
  OpenLineageContext context;
  QueryExecution queryExecution = mock(QueryExecution.class);

  StructType schema =
      new StructType(
          new StructField[] {
            new StructField("a", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("b", IntegerType$.MODULE$, false, new Metadata(new HashMap<>()))
          });

  @BeforeAll
  @SneakyThrows
  public static void beforeAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @AfterAll
  @SneakyThrows
  public static void afterAll() {
    SparkSession$.MODULE$.cleanupAnyExistingSession();
  }

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("ColumnLevelLineage")
            .config("spark.extraListeners", LastQueryExecutionSparkEventListener.class.getName())
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/derby")
            .enableHiveSupport()
            .getOrCreate();

    context =
        OpenLineageContext.builder()
            .sparkSession(Optional.of(spark))
            .sparkContext(spark.sparkContext())
            .openLineage(new OpenLineage(EventEmitter.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .build();

    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/column_non_v2/"), true);

    spark.sql("DROP TABLE IF EXISTS t1");
    spark.sql("DROP TABLE IF EXISTS t2");
    spark.sql("DROP TABLE IF EXISTS t");
  }

  @Test
  public void testNonV2CreateTableAsSelect() {
    spark.sql("CREATE TABLE t1 (a int, b int) LOCATION '/tmp/column_non_v2/t1'");
    spark.sql("INSERT INTO t1 VALUES (1,2)");
    spark.sql("CREATE TABLE t2 LOCATION '/tmp/column_non_v2/t2' AS SELECT * FROM t1");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema).get();

    assertColumnDependsOn(facet, "a", "file", "column_non_v2/t1", "a");
    assertColumnDependsOn(facet, "b", "file", "column_non_v2/t1", "b");
  }

  @Test
  public void testNonV2CatalogInsertIntoTable() {
    spark.sql("CREATE TABLE t1 (a int, b int) LOCATION '/tmp/column_non_v2/t1'");
    spark.sql("INSERT INTO t1 VALUES (1,2)");
    spark.sql("CREATE TABLE t2 (a int, b int) LOCATION '/tmp/column_non_v2/t2'");
    spark.sql("INSERT INTO t2 SELECT * FROM t1");

    LogicalPlan plan = LastQueryExecutionSparkEventListener.getLastExecutedLogicalPlan().get();
    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schema).get();

    assertColumnDependsOn(facet, "a", "file", "column_non_v2/t1", "a");
    assertColumnDependsOn(facet, "b", "file", "column_non_v2/t1", "b");
  }

  private void assertColumnDependsOn(
      OpenLineage.ColumnLineageDatasetFacet facet,
      String outputColumn,
      String expectedNamespace,
      String expectedName,
      String expectedInputField) {
    assertTrue(
        facet.getFields().getAdditionalProperties().get(outputColumn).getInputFields().stream()
            .filter(f -> f.getNamespace().equalsIgnoreCase(expectedNamespace))
            .filter(f -> f.getName().endsWith(expectedName))
            .filter(f -> f.getField().equalsIgnoreCase(expectedInputField))
            .findAny()
            .isPresent());
  }
}
