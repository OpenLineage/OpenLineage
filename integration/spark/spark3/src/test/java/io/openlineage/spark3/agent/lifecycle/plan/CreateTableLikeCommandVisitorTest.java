/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;
import org.apache.spark.sql.internal.SessionState;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.Option$;
import scala.collection.immutable.HashMap;

class CreateTableLikeCommandVisitorTest {

  private SparkSession sparkSession = mock(SparkSession.class);
  private SessionState sessionState = mock(SessionState.class);
  private SessionCatalog sessionCatalog = mock(SessionCatalog.class);
  private String database = "default";
  private TableIdentifier sourceTableIdentifier =
      TableIdentifier$.MODULE$.apply("table", Option.apply(database));
  private TableIdentifier targetTableIdentifier =
      TableIdentifier$.MODULE$.apply("newtable", Option.apply(database));
  private StructType schema =
      new StructType(
          new StructField[] {
            new StructField("key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            new StructField("value", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
          });

  @Test
  @SneakyThrows
  void testCreateTableLikeCommand() {
    CatalogTable sourceCatalogTable = mock(CatalogTable.class);

    when(sparkSession.sparkContext()).thenReturn(mock(SparkContext.class));
    when(sparkSession.sessionState()).thenReturn(sessionState);
    when(sessionState.catalog()).thenReturn(sessionCatalog);
    when(sessionCatalog.getTempViewOrPermanentTableMetadata(sourceTableIdentifier))
        .thenReturn(sourceCatalogTable);
    when(sessionCatalog.defaultTablePath(targetTableIdentifier))
        .thenReturn(new URI("/tmp/warehouse/newtable"));
    when(sourceCatalogTable.schema()).thenReturn(schema);

    CreateTableLikeCommandVisitor visitor =
        new CreateTableLikeCommandVisitor(
            OpenLineageContext.builder()
                .sparkSession(sparkSession)
                .sparkContext(sparkSession.sparkContext())
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build());

    CreateTableLikeCommand command =
        new CreateTableLikeCommand(
            targetTableIdentifier,
            sourceTableIdentifier,
            CatalogStorageFormat.empty(),
            Option$.MODULE$.empty(),
            ScalaConversionUtils.asScalaMapEmpty(),
            false);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(1, datasets.size());
    OpenLineage.OutputDataset outputDataset = datasets.get(0);

    assertEquals(
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE,
        outputDataset.getFacets().getLifecycleStateChange().getLifecycleStateChange());
    assertEquals("/tmp/warehouse/newtable", outputDataset.getName());
    assertEquals("file", outputDataset.getNamespace());
  }

  @Test
  void testJobNameSuffix() {
    CreateTableLikeCommand command = mock(CreateTableLikeCommand.class);
    CreateTableLikeCommandVisitor visitor =
        new CreateTableLikeCommandVisitor(mock(OpenLineageContext.class));
    TableIdentifier tableIdentifier = mock(TableIdentifier.class);
    when(command.targetTable()).thenReturn(tableIdentifier);
    when(tableIdentifier.identifier()).thenReturn("db_t");

    assertThat(visitor.jobNameSuffix(command).get()).isEqualTo("db_t");
    assertThat(visitor.jobNameSuffix(command)).isPresent();

    assertThat(visitor.jobNameSuffix(mock(CreateTableLikeCommand.class))).isEmpty();
  }
}
