/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.lifecycle.ExecutionContext;
import io.openlineage.spark.agent.lifecycle.StaticExecutionContextFactory;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation$;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import scala.Option;
import scala.collection.Map$;
import scala.collection.Seq$;

public class OpenLineageSparkListenerTest {

  @Test
  public void testSqlEventWithJobEventEmitsOnce() {
    SparkSession sparkSession = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    EventEmitter emitter = mock(EventEmitter.class);
    QueryExecution qe = mock(QueryExecution.class);
    LogicalPlan query = UnresolvedRelation$.MODULE$.apply(TableIdentifier.apply("tableName"));
    SparkPlan plan = mock(SparkPlan.class);

    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.appName()).thenReturn("appName");
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
            .openLineage(new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
            .queryExecution(qe)
            .build();
    olContext
        .getOutputDatasetQueryPlanVisitors()
        .add(new InsertIntoHadoopFsRelationVisitor(olContext));
    ExecutionContext executionContext =
        new StaticExecutionContextFactory(emitter)
            .createSparkSQLExecutionContext(1L, emitter, qe, olContext);

    executionContext.start(
        new SparkListenerSQLExecutionStart(
            1L,
            "",
            "",
            "",
            new SparkPlanInfo(
                "name", "string", Seq$.MODULE$.empty(), Map$.MODULE$.empty(), Seq$.MODULE$.empty()),
            1L));
    executionContext.start(
        new SparkListenerJobStart(0, 2L, Seq$.MODULE$.<StageInfo>empty(), new Properties()));

    ArgumentCaptor<OpenLineage.RunEvent> lineageEvent =
        ArgumentCaptor.forClass(OpenLineage.RunEvent.class);

    verify(emitter, times(2)).emit(lineageEvent.capture());
  }
}
