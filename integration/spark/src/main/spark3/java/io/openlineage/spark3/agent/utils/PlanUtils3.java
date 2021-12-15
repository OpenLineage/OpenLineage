package io.openlineage.spark3.agent.utils;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.util.DatasetIdentifier;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogUtils3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.UnsupportedCatalogException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;

/**
 * Utility functions for traversing a {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan} for Spark 3.
 */
@Slf4j
public class PlanUtils3 {

  public static List<OpenLineage.Dataset> getDataset(
      SparkSession session,
      TableCatalog catalog,
      Identifier identifier,
      Map<String, String> properties,
      StructType schema) {
    DatasetIdentifier datasetIdentifier;
    try {
      datasetIdentifier =
          CatalogUtils3.getDatasetIdentifier(session, catalog, identifier, properties);
    } catch (UnsupportedCatalogException ex) {
      log.error(String.format("Catalog %s is unsupported", ex.getMessage()), ex);
      return Collections.emptyList();
    }

    return Collections.singletonList(PlanUtils.getDataset(datasetIdentifier, schema));
  }
}
