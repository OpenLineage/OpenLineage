/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.shared.agent.facets.TableProviderFacet;
import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
import java.util.Map;
import java.util.Optional;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;

public interface CatalogHandler {
  boolean hasClasses();

  boolean isClass(TableCatalog tableCatalog);

  DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties);

  default Optional<TableProviderFacet> getTableProviderFacet(Map<String, String> properties) {
    return Optional.empty();
  }

  /** Try to find string that uniquely identifies version of a dataset. */
  default Optional<String> getDatasetVersion(
      TableCatalog catalog, Identifier identifier, Map<String, String> properties) {
    return Optional.empty();
  }

  String getName();
}
