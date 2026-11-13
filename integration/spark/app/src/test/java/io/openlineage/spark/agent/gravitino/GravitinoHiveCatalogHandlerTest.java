/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.gravitino;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.MissingDatasetIdentifierCatalogException;
import java.util.Collections;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.junit.jupiter.api.Test;

class GravitinoHiveCatalogHandlerTest {
  private final GravitinoHiveCatalogHandler handler = new GravitinoHiveCatalogHandler();

  @Test
  void identifiesOnlyGravitinoHiveCatalog() {
    assertThat(
            GravitinoHiveCatalogHandler.isGravitinoHiveCatalog(
                "org.apache.gravitino.spark.connector.hive.GravitinoHiveCatalogSpark35"))
        .isTrue();
    assertThat(
            GravitinoHiveCatalogHandler.isGravitinoHiveCatalog(
                "org.apache.iceberg.spark.SparkCatalog"))
        .isFalse();
  }

  @Test
  void mapsGravitinoIdentifierToMetalakeNamespace() {
    SparkSession session = mock(SparkSession.class);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    TableCatalog catalog = mock(TableCatalog.class);
    when(session.conf()).thenReturn(conf);
    when(conf.get("spark.sql.gravitino.metalake", "")).thenReturn("spark_metalake");
    when(catalog.name()).thenReturn("hive_catalog");

    DatasetIdentifier dataset =
        handler.getDatasetIdentifier(
            session,
            catalog,
            Identifier.of(new String[] {"spark_db"}, "source_orders_hivetable"),
            Collections.emptyMap());

    assertThat(dataset.getNamespace()).isEqualTo("spark_metalake");
    assertThat(dataset.getName()).isEqualTo("hive_catalog.spark_db.source_orders_hivetable");
  }

  @Test
  void refusesToInventMetalakeWhenConfigurationIsMissing() {
    SparkSession session = mock(SparkSession.class);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    TableCatalog catalog = mock(TableCatalog.class);
    when(session.conf()).thenReturn(conf);
    when(conf.get("spark.sql.gravitino.metalake", "")).thenReturn("");

    assertThatThrownBy(
            () ->
                handler.getDatasetIdentifier(
                    session,
                    catalog,
                    Identifier.of(new String[] {"spark_db"}, "orders"),
                    Collections.emptyMap()))
        .isInstanceOf(MissingDatasetIdentifierCatalogException.class);
  }
}
