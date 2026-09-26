/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark40.agent.utils.UnityCatalogTableTestBase;
import java.util.Collections;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.junit.jupiter.api.Test;

/**
 * Integration test for {@link UnityCatalogHandler} that exercises the handler against a real
 * embedded Unity Catalog server and a Spark 4 session with Delta Lake.
 */
class UnityCatalogHandlerTest extends UnityCatalogTableTestBase {

  @Override
  protected void provisionTable() {
    spark.sql(
        String.format(
            "CREATE TABLE %s.%s.%s (id INT, name STRING) USING delta LOCATION '%s'",
            catalogName, schemaName, tableName, tableLocation));
  }

  @Test
  void testHasClasses() {
    assertThat(new UnityCatalogHandler(context).hasClasses()).isTrue();
  }

  @Test
  void testIsClassRecognizesUnityCatalog() {
    assertThat(new UnityCatalogHandler(context).isClass(unityCatalog)).isTrue();
  }

  @Test
  void testGetDatasetIdentifierUsesLocationWithUnityCatalogSymlink() {
    UnityCatalogHandler handler = new UnityCatalogHandler(context);

    DatasetIdentifier identifier =
        handler.getDatasetIdentifier(
            spark,
            unityCatalog,
            Identifier.of(new String[] {schemaName}, tableName),
            Collections.emptyMap());

    // Primary identifier: the physical table location.
    assertThat(identifier.getNamespace()).startsWith("file");
    assertThat(identifier.getName()).isEqualTo(tableLocation);

    // The Unity Catalog three-part name is attached as a TABLE symlink:
    // unitycatalog://{host}:{port} + {catalog}.{schema}.{table}
    assertThat(identifier.getSymlinks())
        .singleElement()
        .satisfies(
            symlink -> {
              assertThat(symlink.getType()).isEqualTo(DatasetIdentifier.SymlinkType.TABLE);
              assertThat(symlink.getName())
                  .isEqualTo(catalogName + "." + schemaName + "." + tableName);
              assertThat(symlink.getNamespace())
                  .isEqualTo("unitycatalog://" + unityCatalogServer.getUri().getAuthority());
            });
  }

  @Test
  void testGetDatasetVersionReturnsDeltaVersion() {
    UnityCatalogHandler handler = new UnityCatalogHandler(context);

    assertThat(
            handler.getDatasetVersion(
                unityCatalog,
                Identifier.of(new String[] {schemaName}, tableName),
                Collections.emptyMap()))
        .isPresent();
  }
}
