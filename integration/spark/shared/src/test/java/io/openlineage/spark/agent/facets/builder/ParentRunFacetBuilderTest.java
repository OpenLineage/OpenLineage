/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.Seq$;

class ParentRunFacetBuilderTest {

  private static SparkContext sparkContext;
  private static SparkSession sparkSession;
  private static QueryExecution queryExecution;

  @BeforeAll
  public static void setup() {
    sparkContext =
        SparkContext.getOrCreate(
            new SparkConf()
                .setAppName("ParentRunFacetBuilderTest")
                .setMaster("local")
                .set("spark.openlineage.namespace", "testnamespace"));
    sparkSession = SparkSession.builder().sparkContext(sparkContext).getOrCreate();
    Dataset<Row> dataset =
        sparkSession.createDataFrame(
            Arrays.asList(new GenericRow(new Object[] {1, "hello"})),
            new StructType(
                new StructField[] {
                  new StructField(
                      "count",
                      IntegerType$.MODULE$,
                      false,
                      new Metadata(new scala.collection.immutable.HashMap<>())),
                  new StructField(
                      "word",
                      StringType$.MODULE$,
                      false,
                      new Metadata(new scala.collection.immutable.HashMap<>()))
                }));
    queryExecution = dataset.queryExecution();
  }

  @AfterAll
  public static void tearDown() {
    sparkContext.stop();
  }

  @Test
  void testBuildNoParent() {
    ParentRunFacetBuilder builder =
        new ParentRunFacetBuilder(
            OpenLineageContext.builder()
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .sparkContext(sparkContext)
                .build());

    Map<String, RunFacet> facetsMap = new HashMap<>();
    Properties prop = new Properties();
    prop.setProperty("spark.sql.execution.id", "0");
    builder.build(
        new SparkListenerJobStart(0, 2L, Seq$.MODULE$.<StageInfo>empty(), prop), facetsMap::put);
    assertFalse(facetsMap.containsKey("parent"));
  }

  @Test
  void testBuildSparkSqlExecutionParentPresent() {
    OpenLineageContext parentOlContext =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .sparkSession(Optional.of(sparkSession))
            .sparkContext(sparkContext)
            .queryExecution(Optional.of(queryExecution))
            .build();
    OpenLineageContext childOlContext =
        OpenLineageContext.builder()
            .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
            .sparkSession(Optional.of(sparkSession))
            .sparkContext(sparkContext)
            .queryExecution(Optional.of(queryExecution))
            .build();

    ParentRunFacetBuilder builder01 = new ParentRunFacetBuilder(parentOlContext);
    ParentRunFacetBuilder builder02 = new ParentRunFacetBuilder(childOlContext);

    Map<String, RunFacet> facetsMap01 = new HashMap<>();

    // Need an Execution First
    builder01.build(
        new SparkListenerSQLExecutionStart(10L, "", "", "", null, 1L), facetsMap01::put);
    // A SQL Execution Start that doesn't pick up parentRunId and parentJobName
    // from the configruation won't have a parent property.
    assertFalse(facetsMap01.containsKey("parent"));

    // In the case of Databricks' connector to Synapse SQL Pools, it has the
    // spark.sql.execution.parent set to point to a parent execution in the
    // SparkListenerJobStart only.
    Map<String, RunFacet> facetsMap02 = new HashMap<>();
    Properties prop = new Properties();
    prop.setProperty("spark.sql.execution.id", "11");
    prop.setProperty("spark.sql.execution.parent", "10");
    builder02.build(
        new SparkListenerJobStart(10, 2L, Seq$.MODULE$.<StageInfo>empty(), prop), facetsMap02::put);

    // Now the returned facet map contains the parent key and ParentRunFacet
    assert (facetsMap02.containsKey("parent"));
    assertThat(facetsMap02)
        .hasEntrySatisfying(
            "parent",
            facet -> {
              assertEquals(
                  parentOlContext.getRunUuid(), ((ParentRunFacet) facet).getRun().getRunId());
            });
  }
}
