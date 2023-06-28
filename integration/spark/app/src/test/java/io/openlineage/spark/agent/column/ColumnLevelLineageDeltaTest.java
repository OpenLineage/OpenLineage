/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.column;

import static io.openlineage.spark.agent.column.ColumnLevelLineageTestUtils.assertColumnDependsOn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.LastQueryExecutionSparkEventListener;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.column.ColumnLevelLineageUtils;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.LongType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("delta")
public class ColumnLevelLineageDeltaTest {
  @SuppressWarnings("PMD")
  private static final String LOCAL_IP = "127.0.0.1";

  SparkSession spark;
  private static final String T1_EXPECTED_NAME = "/tmp/delta/t1";
  private static final String T2_EXPECTED_NAME = "/tmp/delta/t2";

  QueryExecution queryExecution = mock(QueryExecution.class);

  OpenLineageContext context;
  OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
      openLineage.newSchemaDatasetFacet(
          Arrays.asList(
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("a").type("int").build(),
              openLineage.newSchemaDatasetFacetFieldsBuilder().name("b").type("int").build()));

  @BeforeEach
  @SneakyThrows
  public void beforeEach() {
    spark =
        SparkSession.builder()
            .master("local[*]")
            .appName("DeltaIntegrationTest")
            .config("spark.driver.host", LOCAL_IP)
            .config("spark.driver.bindAddress", LOCAL_IP)
            .config("spark.sql.shuffle.partitions", 1)
            .config("spark.sql.warehouse.dir", "file:/tmp/delta/")
            .config("spark.driver.extraJavaOptions", "-Dderby.system.home=/tmp/delta/derby")
            .config("spark.openlineage.transport.type", "http")
            .config("spark.openlineage.facets.disabled", "spark_unknown;spark.logicalPlan")
            .config("spark.extraListeners", LastQueryExecutionSparkEventListener.class.getName())
            .config("spark.jars.ivy", "/tmp/.ivy2/")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .getOrCreate();

    context =
        OpenLineageContext.builder()
            .sparkSession(Optional.of(spark))
            .sparkContext(spark.sparkContext())
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .queryExecution(queryExecution)
            .build();

    FileSystem.get(spark.sparkContext().hadoopConfiguration())
        .delete(new Path("/tmp/delta/"), true);
  }

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

  @Test
  void testMergeInto() {
    Dataset<Row> dataset =
        spark
            .createDataFrame(
                ImmutableList.of(
                    RowFactory.create(1L, "bat"),
                    RowFactory.create(2L, "mouse"),
                    RowFactory.create(3L, "horse")),
                new StructType(
                    new StructField[] {
                      new StructField("a", LongType$.MODULE$, false, Metadata.empty()),
                      new StructField("b", StringType$.MODULE$, false, Metadata.empty())
                    }))
            .repartition(1);
    dataset.createOrReplaceTempView("temp");

    spark.sql("CREATE TABLE t1 USING delta LOCATION '/tmp/delta/t1' AS SELECT * FROM temp");
    spark.sql("CREATE TABLE t2 USING delta LOCATION '/tmp/delta/t2' AS SELECT * FROM temp");

    spark.sql(
        "MERGE INTO t1 USING t2 ON t1.a = t2.a"
            + " WHEN MATCHED THEN UPDATE SET *"
            + " WHEN NOT MATCHED THEN INSERT *");

    List<LogicalPlan> plans = LastQueryExecutionSparkEventListener.getExecutedLogicalPlans();

    LogicalPlan plan =
        plans.stream()
            .filter(p -> p.getClass().getCanonicalName().endsWith("MergeIntoCommand"))
            .findAny()
            .get();

    when(queryExecution.optimizedPlan()).thenReturn(plan);
    OpenLineage.ColumnLineageDatasetFacet facet =
        ColumnLevelLineageUtils.buildColumnLineageDatasetFacet(context, schemaDatasetFacet).get();

    assertColumnDependsOn(facet, "a", "file", T1_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", "file", T1_EXPECTED_NAME, "b");

    assertColumnDependsOn(facet, "a", "file", T2_EXPECTED_NAME, "a");
    assertColumnDependsOn(facet, "b", "file", T2_EXPECTED_NAME, "b");
  }
}
