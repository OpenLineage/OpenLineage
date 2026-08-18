/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan.catalog;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.clickhouse.spark.ClickHouseCatalog;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.Collections;
import java.util.Optional;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ClickHouseHandlerTest {
  private static final String CLICKHOUSE = "clickhouse";

  private final OpenLineageContext context = mock(OpenLineageContext.class);
  private final ClickHouseHandler handler = new ClickHouseHandler(context);
  private final SparkSession session = mock(SparkSession.class);
  private final RuntimeConfig conf = mock(RuntimeConfig.class);
  private final ClickHouseCatalog catalog = new ClickHouseCatalog();

  @BeforeEach
  void beforeEach() {
    catalog.initialize(CLICKHOUSE, new CaseInsensitiveStringMap(Collections.emptyMap()));
    when(session.conf()).thenReturn(conf);
  }

  @Test
  void testHasClasses() {
    assertThat(handler.hasClasses()).isTrue();
  }

  @Test
  void testIsClass() {
    assertThat(handler.isClass(catalog)).isTrue();
    assertThat(handler.isClass(mock(TableCatalog.class))).isFalse();
  }

  @Test
  void testGetDatasetIdentifier() {
    when(conf.get("spark.sql.catalog.clickhouse.host", "localhost")).thenReturn("ch.example.com");
    when(conf.get("spark.sql.catalog.clickhouse.http_port", "8123")).thenReturn("8123");

    DatasetIdentifier identifier =
        handler.getDatasetIdentifier(
            session,
            catalog,
            Identifier.of(new String[] {"mydb"}, "mytable"),
            Collections.emptyMap());

    assertThat(identifier)
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://ch.example.com:8123")
        .hasFieldOrPropertyWithValue("name", "mydb.mytable");
  }

  @Test
  void testGetDatasetIdentifierWithNonDefaultCatalogName() {
    ClickHouseCatalog otherCatalog = new ClickHouseCatalog();
    otherCatalog.initialize("ch2", new CaseInsensitiveStringMap(Collections.emptyMap()));
    when(conf.get("spark.sql.catalog.ch2.host", "localhost")).thenReturn("other.example.com");
    when(conf.get("spark.sql.catalog.ch2.http_port", "8123")).thenReturn("8124");

    DatasetIdentifier identifier =
        handler.getDatasetIdentifier(
            session,
            otherCatalog,
            Identifier.of(new String[] {"mydb"}, "mytable"),
            Collections.emptyMap());

    assertThat(identifier)
        .hasFieldOrPropertyWithValue("namespace", "clickhouse://other.example.com:8124")
        .hasFieldOrPropertyWithValue("name", "mydb.mytable");
  }

  @Test
  void testGetDatasetIdentifierDefaults() {
    when(conf.get(anyString(), anyString())).thenAnswer(invocation -> invocation.getArgument(1));

    DatasetIdentifier identifier =
        handler.getDatasetIdentifier(
            session,
            catalog,
            Identifier.of(new String[] {"mydb"}, "mytable"),
            Collections.emptyMap());

    assertThat(identifier.getNamespace()).isEqualTo("clickhouse://localhost:8123");
  }

  @Test
  void testGetCatalogDatasetFacet() {
    when(context.getOpenLineage()).thenReturn(new OpenLineage(URI.create("http://localhost")));

    Optional<CatalogHandler.CatalogWithAdditionalFacets> catalogDatasetFacet =
        handler.getCatalogDatasetFacet(catalog, Collections.emptyMap());

    assertThat(catalogDatasetFacet).isPresent();
    OpenLineage.CatalogDatasetFacet facet =
        catalogDatasetFacet.orElseThrow(AssertionError::new).getCatalogDatasetFacet();
    assertThat(facet.getName()).isEqualTo(CLICKHOUSE);
    assertThat(facet.getFramework()).isEqualTo(CLICKHOUSE);
    assertThat(facet.getType()).isEqualTo(CLICKHOUSE);
    assertThat(facet.getSource()).isEqualTo("spark");
  }
}
