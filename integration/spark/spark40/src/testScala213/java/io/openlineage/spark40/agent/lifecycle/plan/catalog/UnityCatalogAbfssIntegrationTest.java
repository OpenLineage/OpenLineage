/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark40.agent.utils.AbfssUnityCatalogTestBase;
import java.net.URI;
import java.util.Collections;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

/**
 * Opt-in integration test that verifies {@link UnityCatalogHandler} produces an {@code abfss://}
 * primary identifier for an Azure Data Lake Storage Gen2 backed table, with the Unity Catalog
 * three-part name attached as a TABLE symlink.
 *
 * <p>Tagged {@code azure}: runs only with {@code -Pazure} against a real storage account.
 */
class UnityCatalogAbfssIntegrationTest extends AbfssUnityCatalogTestBase {

  @Override
  protected void provisionTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s.%s.%s (id BIGINT, value STRING) USING delta LOCATION '%s'",
            catalogName, schemaName, tableName, tableLocation));
  }

  @Test
  void testAbfssTableResolvesToAbfssIdentifierWithUnityCatalogSymlink() throws Exception {
    DatasetIdentifier identifier =
        new UnityCatalogHandler(context)
            .getDatasetIdentifier(
                spark,
                unityCatalog,
                Identifier.of(new String[] {schemaName}, tableName),
                Collections.emptyMap());

    // Primary identifier carries the abfss location:
    // abfss://{container}@{account}.dfs.core.windows.net
    String expectedAbfssNamespace = "abfss://" + new URI(tableLocation).getAuthority();
    assertThat(identifier.getNamespace()).isEqualTo(expectedAbfssNamespace);

    // The Unity Catalog three-part name points at the (embedded) server as a TABLE symlink.
    assertThat(identifier.getSymlinks())
        .singleElement()
        .satisfies(
            symlink -> {
              assertThat(symlink.getType()).isEqualTo(DatasetIdentifier.SymlinkType.TABLE);
              assertThat(symlink.getNamespace())
                  .isEqualTo("unitycatalog://" + unityCatalogServer.getUri().getAuthority());
              assertThat(symlink.getName())
                  .isEqualTo(catalogName + "." + schemaName + "." + tableName);
            });
  }
}
