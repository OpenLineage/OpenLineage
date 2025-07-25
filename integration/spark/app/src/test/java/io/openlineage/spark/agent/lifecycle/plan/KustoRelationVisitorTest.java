/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.LogicalRelationFactory;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.Option;
/**
 * This unit test tests the apply method of KustoRelationVisitor. Therefore it only tests Kusto read
 * operations. It does not cover Kusto writes, as those are routed to the visitor from the
 * SaveIntoDataSourceCommandVisitor
 */
@Slf4j
class KustoCoordinates {
  @SuppressWarnings("PMD") // used by reflection
  private String clusterUrl; // field read from kustoCoordinates by reflection in getNamespace

  @SuppressWarnings("PMD") // used by reflection
  private String database; // field read from kustoCoordinates in by reflection in getNamespace

  @SuppressWarnings("PMD") // used by reflection
  private String getClusterUrl() {
    return clusterUrl;
  }

  @SuppressWarnings("PMD") // used by reflection
  private String getDatabase() {
    return database;
  }

  public KustoCoordinates(String clusterUrl, String database) {
    this.clusterUrl = clusterUrl;
    this.database = database;
  }
}

class MockKustoRelation extends BaseRelation {

  @SuppressWarnings("PMD") // used by reflection
  private final String query; // field read from relation by reflection in getName

  @SuppressWarnings("PMD")
  private final Object kustoCoordinates; // field read from relation by reflection in getNamespace

  @Override
  public SQLContext sqlContext() {
    return null;
  }

  @Override
  public StructType schema() {
    return new StructType(
        new StructField[] {new StructField("name", StringType$.MODULE$, false, null)});
  }

  public MockKustoRelation(String query, String clusterUrl, String database) {
    this.query = query;
    this.kustoCoordinates = new KustoCoordinates(clusterUrl, database);
  }
}

class TestKustoRelationVisitor extends KustoRelationVisitor {
  public TestKustoRelationVisitor(OpenLineageContext context, DatasetFactory factory) {
    super(context, factory);
  }

  @Override
  protected boolean isKustoClass(LogicalPlan plan) {
    return true;
  }
}

class KustoRelationVisitorTest {
  private static final String FIELD_NAME = "name";
  SparkSession session = mock(SparkSession.class);
  OpenLineageContext context = mock(OpenLineageContext.class);

  @BeforeEach
  public void setUp() {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(session.sparkContext()).thenReturn(sparkContext);
    when(context.getOpenLineage()).thenReturn(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI));
  }

  @ParameterizedTest
  @MethodSource("provideInputsForTestKustoRelation")
  void testKustoRelationMain(
      String inputQuery,
      String url,
      String database,
      String expectedName,
      String expectedNamespace,
      int expectedNumOfDatasets) {

    LogicalRelation lr =
              LogicalRelationFactory.create(
                              new MockKustoRelation(inputQuery, url, database),
                              ScalaConversionUtils.fromList(
                                      Collections.singletonList(
                                              new AttributeReference(
                                                      FIELD_NAME,
                                                      StringType$.MODULE$,
                                                      false,
                                                      null,
                                                      ExprId.apply(1L),
                                                      ScalaConversionUtils.<String>asScalaSeqEmpty()))),
                              Option.empty(),
                              false)
                      .orElseThrow(() -> new RuntimeException("Failed to create LogicalRelation"));

      TestKustoRelationVisitor visitor =
        new TestKustoRelationVisitor(
            SparkAgentTestExtension.newContext(session), DatasetFactory.output(context));

    List<OpenLineage.Dataset> datasets = visitor.apply(lr);

    assertEquals(expectedNumOfDatasets, datasets.size());
    OpenLineage.Dataset ds = datasets.get(0);
    assertEquals(expectedNamespace, ds.getNamespace());
    assertEquals(expectedName, ds.getName());
  }

  private static Stream<Arguments> provideInputsForTestKustoRelation() {
    return Stream.of(
        Arguments.of(
            "table01",
            "https://CLUSTERNAME.REGION.kusto.windows.net",
            "DATABASE",
            "table01",
            "azurekusto://CLUSTERNAME.REGION.kusto.windows.net/DATABASE",
            1),
        Arguments.of(
            "table01 | where MinTemp > 19",
            "https://CLUSTERNAME.REGION.kusto.windows.net",
            "DATABASE",
            "COMPLEX",
            "azurekusto://CLUSTERNAME.REGION.kusto.windows.net/DATABASE",
            1));
  }
}
