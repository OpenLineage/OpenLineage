/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.execution.command.RefreshTableCommand;
import org.apache.spark.sql.execution.command.RefreshTableCommand$;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import scala.Option;
import scala.collection.immutable.HashMap;

class RefreshTableCommandVisitorTest {

  private static final String DATABASE = "default";
  private static final String CATALOG = "catalog";
  private static final String TABLE = "table1";

  SparkSession session = mock(SparkSession.class);
  SparkConf sparkConf = new SparkConf();
  SparkContext sparkContext = mock(SparkContext.class);
  SessionCatalog sessionCatalog = mock(SessionCatalog.class);
  SessionState sessionState = mock(SessionState.class);
  CatalogTable catalogTable = mock(CatalogTable.class);

  TableIdentifier tableIdentifier = TableIdentifier$.MODULE$.apply(TABLE, Option.apply(DATABASE));

  StructType schema =
      new StructType(
          new StructField[] {
            new StructField("key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("value", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
          });

  @BeforeEach
  public void setUp() throws NoSuchDatabaseException, NoSuchTableException {
    sparkConf.set("spark.sql.catalogImplementation", "hive");
    sparkConf.set("spark.sql.hive.metastore.uris", "thrift://10.1.0.1:9083");
    sparkConf.set("spark.hadoop.metastore.catalog.default", CATALOG);

    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());
    when(session.sessionState()).thenReturn(sessionState);
    when(sessionState.catalog()).thenReturn(sessionCatalog);
    when(sessionCatalog.getTableMetadata(eq(tableIdentifier))).thenReturn(catalogTable);
  }

  @Test
  @SneakyThrows
  void testRefreshTable() {
    when(sessionCatalog.defaultTablePath(tableIdentifier))
        .thenReturn(new URI("s3://bucket/some-uri"));
    when(sessionCatalog.getTempViewOrPermanentTableMetadata(tableIdentifier))
        .thenReturn(catalogTable);
    when(catalogTable.schema()).thenReturn(schema);
    when(catalogTable.identifier()).thenReturn(tableIdentifier);
    when(catalogTable.qualifiedName()).thenReturn(DATABASE + "." + TABLE);

    try (MockedStatic mocked = mockStatic(SparkSession.class)) {
      mocked.when(SparkSession::getDefaultSession).thenReturn(Option.apply(session));
      mocked.when(SparkSession::getActiveSession).thenReturn(Option.apply(session));
      mocked.when(SparkSession::active).thenReturn(session);

      SparkOpenLineageConfig config = new SparkOpenLineageConfig();
      OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

      OpenLineageContext openLinegeContext =
          OpenLineageContext.builder()
              .sparkSession(session)
              .sparkContext(session.sparkContext())
              .applicationName("app-name")
              .applicationUuid(UUID.fromString("8d99e33e-bbbb-cccc-dddd-18f2343aaaaa"))
              .openLineage(openLineage)
              .meterRegistry(new SimpleMeterRegistry())
              .openLineageConfig(config)
              .build();

      RefreshTableCommandVisitor visitor = new RefreshTableCommandVisitor(openLinegeContext);

      RefreshTableCommand command = RefreshTableCommand$.MODULE$.apply(tableIdentifier);

      assertThat(visitor.isDefinedAt(command)).isTrue();
      List<OpenLineage.InputDataset> datasets = visitor.apply(command);

      assertEquals(1, datasets.size());
      OpenLineage.InputDataset inputDataset = datasets.get(0);

      assertEquals(
          OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.ALTER,
          inputDataset.getFacets().getLifecycleStateChange().getLifecycleStateChange());
      assertEquals("s3://bucket", inputDataset.getNamespace());
      assertEquals("some-uri", inputDataset.getName());
      assertEquals(
          DATABASE + "." + TABLE,
          inputDataset.getFacets().getSymlinks().getIdentifiers().get(0).getName());
    }
  }
}
