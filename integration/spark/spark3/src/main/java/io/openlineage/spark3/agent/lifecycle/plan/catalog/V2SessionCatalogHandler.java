/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import io.openlineage.spark.shared.agent.util.DatasetIdentifier;
import java.util.Map;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog;

public class V2SessionCatalogHandler implements CatalogHandler {

  @Override
  public boolean hasClasses() {
    return true;
  }

  @Override
  public boolean isClass(TableCatalog tableCatalog) {
    return tableCatalog instanceof V2SessionCatalog;
  }

  @Override
  public DatasetIdentifier getDatasetIdentifier(
      SparkSession session,
      TableCatalog tableCatalog,
      Identifier identifier,
      Map<String, String> properties) {
    V2SessionCatalog catalog = (V2SessionCatalog) tableCatalog;
    throw new UnsupportedCatalogException(V2SessionCatalog.class.getCanonicalName());
  }

  @Override
  public String getName() {
    return "session";
  }
}
