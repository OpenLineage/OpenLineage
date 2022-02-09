package io.openlineage.spark3.agent.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.facets.TableProviderFacet;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.api.DatasetFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/**
 * Utility functions for traversing a {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} for Spark 3.
 */
@Slf4j
public class PlanUtils3 {

  public static Optional<DatasetIdentifier> getDatasetIdentifier(
      OpenLineageContext context,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties) {

    SparkSession sparkSession =
        context
            .getSparkSession()
            .orElseThrow(() -> new IllegalArgumentException("SparkSession cannot be empty"));

    try {
      return (Optional.of(
          CatalogUtils3.getDatasetIdentifier(sparkSession, catalog, identifier, properties)));
    } catch (UnsupportedCatalogException ex) {
      log.error(String.format("Catalog %s is unsupported", ex.getMessage()), ex);
      return Optional.empty();
    }
  }

  public static void includeProviderFacet(
      TableCatalog catalog,
      Map<String, String> properties,
      Map<String, OpenLineage.DatasetFacet> facets) {
    Optional<TableProviderFacet> providerFacet =
        CatalogUtils3.getTableProviderFacet(catalog, properties);
    if (providerFacet.isPresent()) {
      facets.put("tableProvider", providerFacet.get());
    }
  }

  public static <D extends OpenLineage.Dataset> List<D> fromDataSourceV2Relation(
      DatasetFactory<D> datasetFactory, OpenLineageContext context, DataSourceV2Relation relation) {
    return fromDataSourceV2Relation(datasetFactory, context, relation, new HashMap<>());
  }

  public static <D extends OpenLineage.Dataset> List<D> fromDataSourceV2Relation(
      DatasetFactory<D> datasetFactory,
      OpenLineageContext context,
      DataSourceV2Relation relation,
      Map<String, OpenLineage.DatasetFacet> facets) {

    if (relation.identifier().isEmpty()) {
      throw new IllegalArgumentException(
          "Couldn't find identifier for dataset in plan " + relation);
    }
    Identifier identifier = relation.identifier().get();

    if (relation.catalog().isEmpty() || !(relation.catalog().get() instanceof TableCatalog)) {
      throw new IllegalArgumentException("Couldn't find catalog for dataset in plan " + relation);
    }
    TableCatalog tableCatalog = (TableCatalog) relation.catalog().get();

    Map<String, String> tableProperties = relation.table().properties();

    includeProviderFacet(tableCatalog, tableProperties, facets);
    Optional<DatasetIdentifier> datasetIdentifier =
        PlanUtils3.getDatasetIdentifier(context, tableCatalog, identifier, tableProperties);

    if (datasetIdentifier.isPresent()) {
      return Collections.singletonList(
          datasetFactory.getDataset(datasetIdentifier.get(), relation.schema(), facets));
    } else {
      return Collections.emptyList();
    }
  }
}
