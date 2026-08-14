/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.util.SparkSessionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import java.util.Optional;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;

class UnityCatalogTableDatasetUtilsTest {

  private static final String CATALOG = "sangeeta_catalog";
  private static final String SCHEMA = "openlineage_dml";
  private static final String TABLE = "copy_into_table";

  @Test
  void resolveUsesV2CatalogWhenLegacyLookupIsUnavailable() throws Exception {
    OpenLineageContext context = mock(OpenLineageContext.class);
    SparkSession session = mock(SparkSession.class);
    TableCatalog tableCatalog = mock(TableCatalog.class);
    Table table = mock(Table.class);
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    Identifier v2Identifier = Identifier.of(new String[] {SCHEMA}, TABLE);
    DatasetIdentifier datasetIdentifier =
        new DatasetIdentifier("unity-catalog/path", "s3://bucket");

    when(tableIdentifier.database()).thenReturn(Option.apply(SCHEMA));
    when(tableIdentifier.table()).thenReturn(TABLE);
    when(table.properties()).thenReturn(Collections.singletonMap("location", "s3://bucket/path"));
    when(table.schema()).thenReturn(new StructType());
    when(tableCatalog.loadTable(v2Identifier)).thenReturn(table);

    try (MockedStatic<SparkSessionUtils> sparkSessionUtils = mockStatic(SparkSessionUtils.class);
        MockedStatic<PlanUtils3> planUtils3 = mockStatic(PlanUtils3.class);
        MockedStatic<MethodUtils> methodUtils = mockStatic(MethodUtils.class)) {
      methodUtils
          .when(() -> MethodUtils.invokeMethod(tableIdentifier, "catalog"))
          .thenReturn(Option.apply(CATALOG));
      sparkSessionUtils
          .when(() -> SparkSessionUtils.catalog(session, CATALOG))
          .thenReturn(Optional.of(tableCatalog));
      planUtils3
          .when(
              () ->
                  PlanUtils3.getDatasetIdentifier(
                      context, tableCatalog, v2Identifier, table.properties()))
          .thenReturn(Optional.of(datasetIdentifier));

      Optional<UnityCatalogTableDatasetUtils.ResolvedTableDataset> resolved =
          UnityCatalogTableDatasetUtils.resolve(context, session, tableIdentifier);

      assertThat(resolved).isPresent();
      assertThat(resolved.get().getDatasetIdentifier()).isEqualTo(datasetIdentifier);
      assertThat(resolved.get().getIdentifier()).isEqualTo(v2Identifier);
    }
  }
}
