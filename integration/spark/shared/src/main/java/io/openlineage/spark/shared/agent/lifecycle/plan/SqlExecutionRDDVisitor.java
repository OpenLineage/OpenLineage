package io.openlineage.spark.shared.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.shared.agent.lifecycle.Rdds;
import io.openlineage.spark.shared.api.DatasetFactory;
import io.openlineage.spark.shared.api.OpenLineageContext;
import lombok.NonNull;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.LogicalRDD;
import org.apache.spark.sql.execution.SQLExecutionRDD;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlExecutionRDDVisitor extends AbstractRDDNodeVisitor<LogicalRDD, InputDataset> {

  public SqlExecutionRDDVisitor(@NonNull OpenLineageContext context) {
    super(context, DatasetFactory.input(context));
  }

  @Override
  public boolean isDefinedAt(LogicalPlan x) {
    return x instanceof LogicalRDD && !findSqlExecution((LogicalRDD) x).isEmpty();
  }

  private Collection<SQLExecutionRDD> findSqlExecution(LogicalRDD logicalRDD) {
    Set<RDD<?>> rdds = Rdds.flattenRDDs(logicalRDD.rdd());
    return rdds.stream()
        .filter(rdd -> rdd instanceof SQLExecutionRDD)
        .map(SQLExecutionRDD.class::cast)
        .collect(Collectors.toList());
  }

  @Override
  public List<InputDataset> apply(LogicalPlan x) {
    return findInputDatasets(Rdds.findFileLikeRdds(((LogicalRDD) x).rdd()), x.schema());
  }
}
