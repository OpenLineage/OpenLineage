/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark34.agent.lifecycle.plan.column;

import static io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import io.openlineage.spark3.agent.utils.PlanUtils3;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.ResolvedIdentifier;
import org.apache.spark.sql.catalyst.plans.logical.DropTable;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class DropTableDatasetBuilderTest {

  private static final String TABLE = "my_table";

  OpenLineageContext openLineageContext =
      OpenLineageContext.builder()
          .sparkSession(mock(SparkSession.class))
          .sparkContext(mock(SparkContext.class))
          .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
          .openLineageConfig(new SparkOpenLineageConfig())
          .meterRegistry(new SimpleMeterRegistry())
          .build();

  DropTableDatasetBuilder builder = new DropTableDatasetBuilder(openLineageContext);

  TableCatalog tableCatalog = mock(TableCatalog.class);
  Identifier identifier = Identifier.of(new String[] {"db"}, TABLE);
  StructType schema = new StructType();
  ResolvedIdentifier resolvedIdentifier = new ResolvedIdentifier(tableCatalog, identifier);
  DropTable dropTable = mock(DropTable.class);

  @BeforeEach
  void setUp() {
    when(dropTable.child()).thenReturn(resolvedIdentifier);
  }

  @Test
  void testIsDefinedAtLogicalPlanWithResolvedIdentifierChild() {
    assertTrue(builder.isDefinedAtLogicalPlan(dropTable));
  }

  @Test
  void testIsDefinedAtLogicalPlanWithNonDropTable() {
    assertFalse(builder.isDefinedAtLogicalPlan(mock(LogicalPlan.class)));
  }

  @Test
  void testIsDefinedAtLogicalPlanWhenChildIsNotResolvedIdentifier() {
    DropTable dropTableWithWrongChild = mock(DropTable.class);
    when(dropTableWithWrongChild.child()).thenReturn(mock(LogicalPlan.class));
    assertFalse(builder.isDefinedAtLogicalPlan(dropTableWithWrongChild));
  }

  @Test
  void testApplyEmitsDropLifecycleState() {
    DatasetIdentifier di = new DatasetIdentifier(TABLE, "db");

    try (MockedStatic<PlanUtils3> mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, new LinkedHashMap<>()))
          .thenReturn(Optional.of(di));

      List<OpenLineage.OutputDataset> datasets = builder.apply(dropTable);

      assertThat(datasets).hasSize(1);
      assertThat(datasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange())
          .isEqualTo(DROP);
      assertThat(datasets.get(0).getName()).isEqualTo(TABLE);
      assertThat(datasets.get(0).getNamespace()).isEqualTo("db");
    }
  }

  @Test
  void testApplyReturnsEmptyWhenNoCatalogPlugin() {
    CatalogPlugin nonTableCatalog = mock(CatalogPlugin.class);
    ResolvedIdentifier nonTableCatalogIdentifier =
        new ResolvedIdentifier(nonTableCatalog, identifier);
    DropTable dropTableWithNonTableCatalog = mock(DropTable.class);
    when(dropTableWithNonTableCatalog.child()).thenReturn(nonTableCatalogIdentifier);

    List<OpenLineage.OutputDataset> datasets = builder.apply(dropTableWithNonTableCatalog);

    assertThat(datasets).isEmpty();
  }

  @Test
  void testApplyReturnsEmptyWhenNoDatasetIdentifierFound() {
    try (MockedStatic<PlanUtils3> mocked = mockStatic(PlanUtils3.class)) {
      when(PlanUtils3.getDatasetIdentifier(
              openLineageContext, tableCatalog, identifier, new LinkedHashMap<>()))
          .thenReturn(Optional.empty());

      List<OpenLineage.OutputDataset> datasets = builder.apply(dropTable);

      assertThat(datasets).isEmpty();
    }
  }
}
