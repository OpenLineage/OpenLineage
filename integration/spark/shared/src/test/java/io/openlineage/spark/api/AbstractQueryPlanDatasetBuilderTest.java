/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.Versions;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;

class AbstractQueryPlanDatasetBuilderTest {

  private static final String LOCAL = "local";

  @Test
  void testIsDefinedOnSparkListenerEvent() {
    SparkSession session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master(LOCAL)
            .getOrCreate();
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    InputDataset expected = openLineage.newInputDataset("namespace", "the_name", null, null);

    OpenLineageContext context = createContext(session, openLineage);
    AbstractQueryPlanDatasetBuilder<SparkListenerJobEnd, LocalRelation, InputDataset> builder =
        new AbstractQueryPlanDatasetBuilder<SparkListenerJobEnd, LocalRelation, InputDataset>(
            context, true) {
          @Override
          public List<InputDataset> apply(LocalRelation logicalPlan) {
            return Collections.singletonList(expected);
          }
        };

    Assertions.assertFalse(
        ((PartialFunction) builder).isDefinedAt(new SparkListenerStageCompleted(null)));
    Assertions.assertTrue(
        ((PartialFunction) builder).isDefinedAt(new SparkListenerJobEnd(1, 2, null)));
  }

  @Test
  void testApplyOnSparkListenerEvent() {
    SparkSession session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master(LOCAL)
            .getOrCreate();
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    InputDataset expected = openLineage.newInputDataset("namespace", "the_name", null, null);

    OpenLineageContext context = createContext(session, openLineage);
    MyNonGenericInputDatasetBuilder builder =
        new MyNonGenericInputDatasetBuilder(context, true, expected);

    SparkListenerJobEnd jobEnd = new SparkListenerJobEnd(1, 2, null);
    Assertions.assertTrue(((PartialFunction) builder).isDefinedAt(jobEnd));
    Collection<InputDataset> datasets = builder.apply(jobEnd);
    assertThat(datasets).isNotEmpty().contains(expected);
  }

  static class MyNonGenericInputDatasetBuilder
      extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, LocalRelation, InputDataset> {
    InputDataset expected;

    public MyNonGenericInputDatasetBuilder(
        OpenLineageContext context, boolean searchDependencies, InputDataset expected) {
      super(context, searchDependencies);
      this.expected = expected;
    }

    @Override
    public List<InputDataset> apply(LocalRelation logicalPlan) {
      return Collections.singletonList(expected);
    }
  }

  @Test
  void testApplyOnBuilderWithGenericArg() {
    SparkSession session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master(LOCAL)
            .getOrCreate();
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    InputDataset expected = openLineage.newInputDataset("namespace", "the_name", null, null);

    OpenLineageContext context = createContext(session, openLineage);
    MyGenericArgInputDatasetBuilder<SparkListenerJobEnd> builder =
        new MyGenericArgInputDatasetBuilder<>(context, true, expected);

    SparkListenerJobEnd jobEnd = new SparkListenerJobEnd(1, 2, null);

    // Even though our instance of builder is parameterized with SparkListenerJobEnd, it's not
    // *compiled* with that argument, so the isDefinedAt method fails to resolve the type arg
    Assertions.assertFalse(((PartialFunction) builder).isDefinedAt(jobEnd));
  }

  @Test
  void testApplyWhenExceptionIsThrown() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getQueryExecution()).thenReturn(Optional.of(mock(QueryExecution.class)));
    AbstractQueryPlanDatasetBuilder<SparkListenerJobEnd, LocalRelation, InputDataset> builder =
        new AbstractQueryPlanDatasetBuilder<SparkListenerJobEnd, LocalRelation, InputDataset>(
            context, false) {

          @Override
          public List<InputDataset> apply(LocalRelation localRelation) {
            return Collections.emptyList();
          }

          @Override
          protected boolean isDefinedAtLogicalPlan(LogicalPlan logicalPlan) {
            throw new RuntimeException("some error");
          }
        };

    try {
      ((PartialFunction) builder).apply(mock(SparkListenerJobEnd.class));
    } catch (Exception e) {
      fail("Exception should not be thrown");
    }
  }

  static class MyGenericArgInputDatasetBuilder<E extends SparkListenerEvent>
      extends AbstractQueryPlanDatasetBuilder<E, LocalRelation, InputDataset> {
    InputDataset expected;

    public MyGenericArgInputDatasetBuilder(
        OpenLineageContext context, boolean searchDependencies, InputDataset expected) {
      super(context, searchDependencies);
      this.expected = expected;
    }

    @Override
    public List<InputDataset> apply(LocalRelation logicalPlan) {
      return Collections.singletonList(expected);
    }
  }

  private OpenLineageContext createContext(SparkSession session, OpenLineage openLineage) {
    QueryExecution queryExecution =
        session
            .createDataFrame(
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
                    }))
            .queryExecution();

    OpenLineageContext context =
        OpenLineageContext.builder()
            .sparkContext(
                SparkContext.getOrCreate(new SparkConf().setAppName("test").setMaster(LOCAL)))
            .openLineage(openLineage)
            .queryExecution(queryExecution)
            .build();
    return context;
  }
}
