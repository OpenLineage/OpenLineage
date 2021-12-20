package io.openlineage.spark.agent;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import io.openlineage.spark.agent.lifecycle.SparkSQLExecutionContext;
import io.openlineage.spark.agent.lifecycle.plan.InsertIntoHadoopFsRelationVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.QueryExecution;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.SparkPlanInfo;
import org.apache.spark.sql.execution.datasources.InsertIntoHadoopFsRelationCommand;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import scala.Option;
import scala.collection.Map$;
import scala.collection.Seq$;

@ExtendWith(SparkAgentTestExtension.class)
public class OpenLineageSparkListenerTest {
  @Test
  public void testSqlEventWithJobEventEmitsOnce(SparkSession sparkSession) {
    EventEmitter emitter = mock(EventEmitter.class);
    QueryExecution qe = mock(QueryExecution.class);
    LogicalPlan query = mock(LogicalPlan.class);
    SparkPlan plan = mock(SparkPlan.class);

    when(query.schema())
        .thenReturn(
            new StructType(
                new StructField[] {
                  new StructField("key", IntegerType$.MODULE$, false, Metadata.empty())
                }));

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
    when(plan.sparkContext()).thenReturn(SparkContext.getOrCreate());
    when(plan.nodeName()).thenReturn("execute");

    OpenLineageContext olContext =
        new OpenLineageContext(
            Optional.of(sparkSession),
            sparkSession.sparkContext(),
            new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI),
            new ArrayList<>(),
            new ArrayList<>(),
            Optional.empty());
    olContext
        .getOutputDatasetQueryPlanVisitors()
        .add(new InsertIntoHadoopFsRelationVisitor(olContext));
    SparkSQLExecutionContext executionContext =
        new SparkSQLExecutionContext(1L, emitter, qe, olContext);

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

    verify(emitter, times(1)).emit(lineageEvent.capture());
  }
}
