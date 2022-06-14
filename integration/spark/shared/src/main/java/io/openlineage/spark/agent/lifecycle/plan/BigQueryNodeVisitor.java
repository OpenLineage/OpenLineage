/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import com.google.cloud.spark.bigquery.BigQueryRelation;
import com.google.cloud.spark.bigquery.BigQueryRelationProvider;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.SaveIntoDataSourceCommand;
import org.apache.spark.sql.sources.CreatableRelationProvider;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * {@link LogicalPlan} visitor that matches {@link BigQueryRelation}s or {@link
 * SaveIntoDataSourceCommand}s that use a {@link BigQueryRelationProvider}. This function extracts a
 * {@link OpenLineage.Dataset} from the BigQuery table referenced by the relation. The convention
 * used for naming is a URI of <code>
 * bigquery://&lt;projectId&gt;.&lt;.datasetId&gt;.&lt;tableName&gt;</code> . The namespace for
 * bigquery tables is always <code>bigquery</code> and the name is the FQN.
 *
 * @param <D> the type of {@link OpenLineage.Dataset} created by this visitor
 */
public class BigQueryNodeVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {
  private static final String BIGQUERY_NAMESPACE = "bigquery";
  private final DatasetFactory<D> factory;

  public BigQueryNodeVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  public static boolean hasBigQueryClasses() {
    try {
      BigQueryNodeVisitor.class
          .getClassLoader()
          .loadClass("com.google.cloud.spark.bigquery.BigQueryRelation");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan plan) {
    return bigQuerySupplier(plan).isPresent();
  }

  private Optional<Supplier<BigQueryRelation>> bigQuerySupplier(LogicalPlan plan) {
    // SaveIntoDataSourceCommand is a special case because it references a CreatableRelationProvider
    // Every other write instance references a LogicalRelation(BigQueryRelation, _, _, _)
    SQLContext sqlContext = context.getSparkSession().get().sqlContext();
    if (plan instanceof SaveIntoDataSourceCommand) {
      SaveIntoDataSourceCommand saveCommand = (SaveIntoDataSourceCommand) plan;
      CreatableRelationProvider relationProvider = saveCommand.dataSource();
      if (relationProvider instanceof BigQueryRelationProvider) {
        return Optional.of(
            () ->
                (BigQueryRelation)
                    ((BigQueryRelationProvider) relationProvider)
                        .createRelation(sqlContext, saveCommand.options(), saveCommand.schema()));
      }
    } else {
      if (plan instanceof LogicalRelation
          && ((LogicalRelation) plan).relation() instanceof BigQueryRelation) {
        return Optional.of(() -> (BigQueryRelation) ((LogicalRelation) plan).relation());
      }
    }
    return Optional.empty();
  }

  @Override
  public List<D> apply(LogicalPlan x) {
    return bigQuerySupplier(x)
        .map(
            s -> {
              BigQueryRelation relation = s.get();
              String name = relation.tableName();
              return Collections.singletonList(
                  factory.getDataset(name, BIGQUERY_NAMESPACE, relation.schema()));
            })
        .orElse(null);
  }
}
