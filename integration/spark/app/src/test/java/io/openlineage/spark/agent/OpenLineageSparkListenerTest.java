/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.Environment;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.filters.EventFilterUtils;
import io.openlineage.spark.agent.lifecycle.ContextFactory;
import io.openlineage.spark.agent.lifecycle.ExecutionContext;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URISyntaxException;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation$;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Map$;
import scala.collection.Seq$;

class OpenLineageSparkListenerTest {

    @Test
    void testSqlEventWithJobEventEmitsOnce() {
        SparkSession sparkSession = mock(SparkSession.class);
        SparkContext sparkContext = mock(SparkContext.class);
        EventEmitter emitter = mock(EventEmitter.class);
        QueryExecution qe = mock(QueryExecution.class);
        LogicalPlan query = UnresolvedRelation$.MODULE$.apply(TableIdentifier.apply("tableName"));
        SparkPlan plan = mock(SparkPlan.class);

        when(sparkSession.sparkContext()).thenReturn(sparkContext);
        when(sparkContext.appName()).thenReturn("appName");
        when(sparkContext.getConf()).thenReturn(new SparkConf());
        when(qe.optimizedPlan())
                .thenReturn(
                        new InsertIntoHadoopFsRelationCommand(
                                new Path("file:///tmp/dir"),
                                null,
                                false,
                                Seq$.MODULE$.empty(),
                                Option.empty(),
                                null,
                                Map$.MODULE$.empty(),
                                query,
                                SaveMode.Overwrite,
                                Option.empty(),
                                Option.empty(),
                                Seq$.MODULE$.<String>empty()));

        when(qe.executedPlan()).thenReturn(plan);
        when(plan.sparkContext()).thenReturn(sparkContext);
        when(plan.nodeName()).thenReturn("execute");

        OpenLineageContext olContext =
                OpenLineageContext.builder()
                        .sparkSession(Optional.of(sparkSession))
                        .sparkContext(sparkSession.sparkContext())
                        .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                        .queryExecution(qe)
                        .build();
        olContext
                .getOutputDatasetQueryPlanVisitors()
                .add(new InsertIntoHadoopFsRelationVisitor(olContext));
        ExecutionContext executionContext =
                new StaticExecutionContextFactory(emitter)
                        .createSparkSQLExecutionContext(1L, emitter, qe, olContext);

        SparkListenerSQLExecutionStart event = mock(SparkListenerSQLExecutionStart.class);
        when(event.sparkPlanInfo())
                .thenReturn(
                        new SparkPlanInfo(
                                "name",
                                "string",
                                Seq$.MODULE$.empty(),
                                Map$.MODULE$.empty(),
                                Seq$.MODULE$.empty()));
        when(event.executionId()).thenReturn(1L);
        try (MockedStatic<EventFilterUtils> utils = mockStatic(EventFilterUtils.class)) {
            utils.when(() -> EventFilterUtils.isDisabled(olContext, event)).thenReturn(false);
            executionContext.start(event);
        }

        ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
                ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

        verify(emitter, times(1)).emit(lineageEvent.capture());
    }

    @Test
    void testOpenlineageDisableDisablesExecution() throws URISyntaxException {
        try (MockedStatic mocked = mockStatic(Environment.class)) {
            when(Environment.getEnvironmentVariable("OPENLINEAGE_DISABLED")).thenReturn("true");

            ContextFactory contextFactory = mock(ContextFactory.class);

            OpenLineageSparkListener.init(contextFactory);
            OpenLineageSparkListener listener = new OpenLineageSparkListener();

            listener.onJobStart(
                    new SparkListenerJobStart(0, 2L, Seq$.MODULE$.<StageInfo>empty(), new Properties()));

            verify(contextFactory, never()).createSparkSQLExecutionContext(anyLong());
        }
    }

    @Test
    void testJdbcRelationQuery() {
        SparkSession sparkSession = mock(SparkSession.class);
        SparkContext sparkContext = mock(SparkContext.class);
        EventEmitter emitter = mock(EventEmitter.class);
        QueryExecution qe = mock(QueryExecution.class);

        JDBCRelation jdbcRelation = createJdbcRelation("select * from table1 limit 1",
                "jdbc:mysql://localhost:3306/my_db", sparkSession);

        LogicalPlan query = createLogicalPlan(jdbcRelation);
        SparkPlan plan = mock(SparkPlan.class);

        when(sparkSession.sparkContext()).thenReturn(sparkContext);
        when(sparkContext.appName()).thenReturn("appName");
        when(sparkContext.getConf()).thenReturn(new SparkConf());
        when(qe.optimizedPlan())
                .thenReturn(
                        new InsertIntoHadoopFsRelationCommand(
                                new Path("file:///tmp/dir"),
                                null,
                                false,
                                Seq$.MODULE$.empty(),
                                Option.empty(),
                                null,
                                Map$.MODULE$.empty(),
                                query,
                                SaveMode.Overwrite,
                                Option.empty(),
                                Option.empty(),
                                Seq$.MODULE$.<String>empty()));

        when(qe.executedPlan()).thenReturn(plan);
        when(plan.sparkContext()).thenReturn(sparkContext);
        when(plan.nodeName()).thenReturn("execute");

        OpenLineageContext olContext =
                OpenLineageContext.builder()
                        .sparkSession(Optional.of(sparkSession))
                        .sparkContext(sparkSession.sparkContext())
                        .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                        .queryExecution(qe)
                        .build();
        olContext
                .getOutputDatasetQueryPlanVisitors()
                .add(new InsertIntoHadoopFsRelationVisitor(olContext));
        ExecutionContext executionContext =
                new StaticExecutionContextFactory(emitter)
                        .createSparkSQLExecutionContext(1L, emitter, qe, olContext);

        SparkListenerSQLExecutionStart event = mock(SparkListenerSQLExecutionStart.class);
        when(event.sparkPlanInfo())
                .thenReturn(
                        new SparkPlanInfo(
                                "name",
                                "string",
                                Seq$.MODULE$.empty(),
                                Map$.MODULE$.empty(),
                                Seq$.MODULE$.empty()));
        when(event.executionId()).thenReturn(1L);
        try (MockedStatic<EventFilterUtils> utils = mockStatic(EventFilterUtils.class)) {
            utils.when(() -> EventFilterUtils.isDisabled(olContext, event)).thenReturn(false);
            executionContext.start(event);
        }

        ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
                ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

        verify(emitter, times(1)).emit(lineageEvent.capture());
        assertEquals("my_db.table1", lineageEvent.getValue().getInputs().get(0).getFacets().getSymlinks().getIdentifiers().get(0).getName());
    }

    private JDBCRelation createJdbcRelation(String query, String url, SparkSession sparkSession) {

        scala.collection.immutable.Map<String, String> params =
                        scala.collection.immutable.Map$.MODULE$
                                .<String, String>newBuilder()
                                .$plus$eq(Tuple2.apply("driver", Driver.class.getName()))
                                .result();
        return new JDBCRelation(
                new StructType(
                        new StructField[]{
                                new StructField("key", IntegerType$.MODULE$, false, null),
                                new StructField("value", StringType$.MODULE$, false, null)
                        }),
                new Partition[]{},
                new JDBCOptions(
                        url,
                        query,
                        params),
                sparkSession);
    }

    private LogicalPlan createLogicalPlan(BaseRelation relation) {
        return new LogicalRelation(relation,
                Seq$.MODULE$
                        .<AttributeReference>newBuilder()
                        .$plus$eq(
                                new AttributeReference(
                                        "key",
                                        IntegerType$.MODULE$,
                                        false,
                                        null,
                                        ExprId.apply(1L),
                                        Seq$.MODULE$.<String>empty()))
                        .$plus$eq(
                                new AttributeReference(
                                        "value",
                                        StringType$.MODULE$,
                                        false,
                                        null,
                                        ExprId.apply(2L),
                                        Seq$.MODULE$.<String>empty()))
                        .result(),
                Option.empty(),
                false);
    }
}
