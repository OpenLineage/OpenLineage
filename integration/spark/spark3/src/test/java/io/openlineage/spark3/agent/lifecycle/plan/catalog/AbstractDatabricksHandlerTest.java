/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import io.openlineage.spark.agent.util.DatabricksUtils;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collections;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AbstractDatabricksHandlerTest {

  private static class TestHandler extends AbstractDatabricksHandler {
    TestHandler(OpenLineageContext context, String className) {
      super(context, className);
    }

    @Override
    public String getName() {
      return "test";
    }
  }

  OpenLineageContext context = mock(OpenLineageContext.class);
  SparkSession sparkSession = mock(SparkSession.class);
  SparkContext sparkContext = mock(SparkContext.class);
  SparkConf sparkConf = new SparkConf();
  TableCatalog tableCatalog = mock(TableCatalog.class);

  @BeforeEach
  void setUp() {
    sparkConf.set("spark.databricks.workspaceUrl", "https://databricks-workspace.example.com");
    sparkConf.set("spark.databricks.unityCatalog.enabled", "true");
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());
    when(sparkSession.sparkContext()).thenReturn(sparkContext);
    when(tableCatalog.name()).thenReturn("main_catalog");
  }

  @Test
  void getDatasetIdentifierUsesUnityCatalogSymlinkWhenUnityCatalogEnabled() {
    TestHandler handler =
        new TestHandler(context, "com.databricks.sql.managedcatalog.UnityCatalogV2Proxy");
    Identifier identifier = Identifier.of(new String[] {"default"}, "employee_processed_ext");

    DatasetIdentifier datasetIdentifier =
        handler.getDatasetIdentifier(
            sparkSession,
            tableCatalog,
            identifier,
            Collections.singletonMap("location", "s3://bucket/unity-catalog/table_location"));

    assertThat(datasetIdentifier.getSymlinks()).hasSize(1);
    assertThat(datasetIdentifier.getSymlinks().get(0))
        .hasFieldOrPropertyWithValue("name", "main_catalog.default.employee_processed_ext")
        .hasFieldOrPropertyWithValue("namespace", DatabricksUtils.UNITY_CATALOG_SYMLINK_NAMESPACE)
        .hasFieldOrPropertyWithValue("type", SymlinkType.TABLE);
  }
}
