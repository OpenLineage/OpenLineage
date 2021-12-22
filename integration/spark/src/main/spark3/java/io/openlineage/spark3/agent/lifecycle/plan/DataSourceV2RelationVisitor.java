package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.lifecycle.plan.DatasetSource;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.QueryPlanVisitor;
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
public class DataSourceV2RelationVisitor<D extends OpenLineage.Dataset>
    extends QueryPlanVisitor<LogicalPlan, D> {

  private enum Provider {
    ICEBERG,
    AZURECOSMOS,
    DELTA,
    UNKNOWN;
  }

  private final DatasetFactory<D> factory;

  public DataSourceV2RelationVisitor(OpenLineageContext context, DatasetFactory<D> factory) {
    super(context);
    this.factory = factory;
  }

  @Override
  public boolean isDefinedAt(LogicalPlan logicalPlan) {
    return logicalPlan instanceof DataSourceV2Relation
        && !findDatasetProvider(logicalPlan).equals(Provider.UNKNOWN);
  }
  
  private String providerPropertyOrTableMetadata(Table table) {
    if (table.properties().containsKey("provider")
        && table.properties().getOrDefault("provider", null) != null) {
      return table.properties().get("provider");
    } else if (table.name().startsWith("com.azure.cosmos")) {
      return "AZURECOSMOS";
    }
    return null;
  }

  private Provider findDatasetProvider(LogicalPlan plan) {
    return Optional.of(plan)
        .filter(x -> x instanceof DataSourceV2Relation)
        .map(x -> (DataSourceV2Relation) x)
        .map(DataSourceV2Relation::table)
        .map(this::providerPropertyOrTableMetadata)
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

  private D findDatasetForIceberg(DataSourceV2Relation relation) {
    Map<String, String> properties = relation.table().properties();

    String namespace = properties.getOrDefault("location", null);
    String format = properties.getOrDefault("format", null);
    // Should not happen - we're inside proper iceberg table.
    if (namespace == null || format == null) {
      return null;
    }
    namespace = namespace.startsWith("/") ? "file://" + namespace : namespace;
    return factory.getDataset(
        relation.table().name(),
        namespace,
        new OpenLineage.DatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(context.getOpenLineage(), relation.schema()))
            .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), namespace))
            .put(
                "table_provider",
                new TableProviderFacet(
                    Provider.ICEBERG.name().toLowerCase(), format.replace("iceberg/", "")))
            .build());
  }

  private D findDatasetForAzureCosmos(DataSourceV2Relation relation) {
    String namespace = "azurecosmos";
    String relationName = relation.table().name().replace("com.azure.cosmos.spark.items.", "");
    int expectedParts = 3;
    String[] tableParts = relationName.split("\\.", expectedParts);
    String tableName;
    if (tableParts.length != expectedParts) {
      tableName = relationName;
    } else {
      tableName =
          String.format(
              "https://%s.documents.azure.com/dbs/%s/colls/%s",
              tableParts[0], tableParts[1], tableParts[2]);
    }
    return factory.getDataset(
      tableName,
        namespace,
      new OpenLineage.DatasetFacetsBuilder()
          .schema(PlanUtils.schemaFacet(context.getOpenLineage(), relation.schema()))
          .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), namespace))
          .put(
              "table_provider",
              new TableProviderFacet(
                Provider.AZURECOSMOS.name().toLowerCase(), "json")) // Cosmos is always json
          .build());
  }

  private D findDatasetForDelta(DataSourceV2Relation relation) {
    Map<String, String> properties = relation.table().properties();

    String namespace = properties.getOrDefault("location", null);
    String format = properties.getOrDefault("format", null);
    // Should not happen - we're inside proper delta table.
    if (namespace == null || format == null) {
      return null;
    }

    return factory.getDataset(
        relation.table().name(),
        namespace,
        new OpenLineage.DatasetFacetsBuilder()
            .schema(PlanUtils.schemaFacet(context.getOpenLineage(), relation.schema()))
            .dataSource(PlanUtils.datasourceFacet(context.getOpenLineage(), namespace))
            .put(
                "table_provider",
                new TableProviderFacet(
                    Provider.DELTA.name().toLowerCase(), "parquet")) // Delta is always parquet
            .build());
  }

  @Override
  public List<D> apply(LogicalPlan logicalPlan) {
    Provider provider = findDatasetProvider(logicalPlan);
    DataSourceV2Relation x = (DataSourceV2Relation) logicalPlan;

    switch (provider) {
      case ICEBERG:
        return nullableSingletonList(findDatasetForIceberg(x));
      case DELTA:
        return nullableSingletonList(findDatasetForDelta(x));
      case AZURECOSMOS:
        return nullableSingletonList(findDatasetForAzureCosmos(x));
      default:
        throw new RuntimeException("Couldn't find provider for dataset in plan " + logicalPlan);
    }
  }

  private List<D> nullableSingletonList(D singleton) {
    if (singleton == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(singleton);
  }
}
