package io.openlineage.spark3.agent.lifecycle.plan;

import static io.openlineage.spark.agent.util.PlanUtils.datasourceFacet;
import static io.openlineage.spark.agent.util.PlanUtils.schemaFacet;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.lifecycle.plan.DatasetSource;
import io.openlineage.spark.agent.lifecycle.plan.QueryPlanVisitor;
import io.openlineage.spark.agent.util.PlanUtils;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table} that implement the {@link DatasetSource} interface.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table).
 */
public class DatasetSourceVisitor extends QueryPlanVisitor<LogicalPlan, OpenLineage.Dataset> {

  private static final String ICEBERG = "iceberg";

  @Override
  public boolean isDefinedAt(LogicalPlan logicalPlan) {
    return findDatasetProvider(logicalPlan).equals(ICEBERG)
        || findDefaultDatasetSource(logicalPlan).isPresent();
  }

  private Optional<DatasetSource> findDefaultDatasetSource(LogicalPlan plan) {
    if (plan instanceof LogicalRelation) {
      if (((LogicalRelation) plan).relation() instanceof DatasetSource) {
        return Optional.of((DatasetSource) ((LogicalRelation) plan).relation());
      }
    } else if (plan instanceof DataSourceV2Relation) {
      DataSourceV2Relation relation = (DataSourceV2Relation) plan;

      if (relation.table() instanceof DatasetSource) {
        return Optional.of((DatasetSource) relation.table());
      }
    }
    return Optional.empty();
  }

  private String findDatasetProvider(LogicalPlan plan) {
    return Optional.of(plan)
        .filter(x -> x instanceof DataSourceV2Relation)
        .map(x -> (DataSourceV2Relation) x)
        .map(DataSourceV2Relation::table)
        .map(Table::properties)
        .map(properties -> properties.get("provider"))
        .map(String::toLowerCase)
        .orElse("unknown");
  }

  private OpenLineage.Dataset findDatasetForIceberg(DataSourceV2Relation relation) {
    Map<String, String> properties = relation.table().properties();

    String namespace = properties.getOrDefault("location", "unknown");
    namespace = namespace.startsWith("/") ? "file://" + namespace : namespace;
    return PlanUtils.getDataset(
        relation.table().name(),
        namespace,
        new OpenLineage.DatasetFacetsBuilder()
            .schema(schemaFacet(relation.schema()))
            .dataSource(datasourceFacet(namespace))
            .put(
                "table_provider",
                new TableProviderFacet(
                    ICEBERG, properties.getOrDefault("format", "unknown").replace("iceberg/", "")))
            .build());
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan logicalPlan) {
    String provider = findDatasetProvider(logicalPlan);

    if (provider.equals("iceberg")) {
      return Collections.singletonList(findDatasetForIceberg((DataSourceV2Relation) logicalPlan));
    } else {
      DatasetSource datasetSource =
          findDefaultDatasetSource(logicalPlan)
              .orElseThrow(
                  () -> new RuntimeException("Couldn't find DatasetSource in plan " + logicalPlan));

      return Collections.singletonList(
          PlanUtils.getDataset(
              datasetSource.name(),
              datasetSource.namespace(),
              PlanUtils.datasetFacet(logicalPlan.schema(), datasetSource.namespace())));
    }
  }
}
