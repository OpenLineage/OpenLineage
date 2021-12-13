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
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Find {@link org.apache.spark.sql.sources.BaseRelation}s and {@link
 * org.apache.spark.sql.connector.catalog.Table} that implement the {@link DatasetSource} interface.
 *
 * <p>Note that while the {@link DataSourceV2Relation} is a {@link
 * org.apache.spark.sql.catalyst.analysis.NamedRelation}, the returned name is that of the source,
 * not the specific dataset (e.g., "bigquery" not the table).
 */
@Slf4j
public class DataSourceV2RelationVisitor
    extends QueryPlanVisitor<LogicalPlan, OpenLineage.Dataset> {

  private enum Provider {
    ICEBERG,
    DELTA,
    UNKNOWN;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan logicalPlan) {
    return logicalPlan instanceof DataSourceV2Relation
        && !findDatasetProvider(logicalPlan).equals(Provider.UNKNOWN);
  }

  private Provider findDatasetProvider(LogicalPlan plan) {
    return Optional.of(plan)
        .filter(x -> x instanceof DataSourceV2Relation)
        .map(x -> (DataSourceV2Relation) x)
        .map(DataSourceV2Relation::table)
        .map(Table::properties)
        .map(properties -> properties.get("provider"))
        .map(String::toUpperCase)
        .map(
            provider -> {
              try {
                return Provider.valueOf(provider);
              } catch (IllegalArgumentException e) {
                return Provider.UNKNOWN;
              }
            })
        .orElse(Provider.UNKNOWN);
  }

  private OpenLineage.Dataset findDatasetForIceberg(DataSourceV2Relation relation) {
    Map<String, String> properties = relation.table().properties();

    String namespace = properties.getOrDefault("location", null);
    String format = properties.getOrDefault("format", null);
    // Should not happen - we're inside proper iceberg table.
    if (namespace == null || format == null) {
      return null;
    }
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
                    Provider.ICEBERG.name().toLowerCase(), format.replace("iceberg/", "")))
            .build());
  }

  private OpenLineage.Dataset findDatasetForDelta(DataSourceV2Relation relation) {
    Map<String, String> properties = relation.table().properties();

    String namespace = properties.getOrDefault("location", null);
    String format = properties.getOrDefault("format", null);
    // Should not happen - we're inside proper delta table.
    if (namespace == null || format == null) {
      return null;
    }

    return PlanUtils.getDataset(
        relation.table().name(),
        namespace,
        new OpenLineage.DatasetFacetsBuilder()
            .schema(schemaFacet(relation.schema()))
            .dataSource(datasourceFacet(namespace))
            .put(
                "table_provider",
                new TableProviderFacet(
                    Provider.DELTA.name().toLowerCase(), "parquet")) // Delta is always parquet
            .build());
  }

  @Override
  public List<OpenLineage.Dataset> apply(LogicalPlan logicalPlan) {
    Provider provider = findDatasetProvider(logicalPlan);
    DataSourceV2Relation x = (DataSourceV2Relation) logicalPlan;

    switch (provider) {
      case ICEBERG:
        return nullableSingletonList(findDatasetForIceberg(x));
      case DELTA:
        return nullableSingletonList(findDatasetForDelta(x));
      default:
        throw new RuntimeException("Couldn't find provider for dataset in plan " + logicalPlan);
    }
  }

  private <T> List<T> nullableSingletonList(T singleton) {
    if (singleton == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(singleton);
  }
}
