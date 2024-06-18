/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark2.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableAsSelectCommandVisitor;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.Map$;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

class CreateDataSourceTableAsSelectCommandVisitorTest {

  SparkSession session = mock(SparkSession.class);

  @BeforeEach
  public void setUp() {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.getConf()).thenReturn(new SparkConf());
    when(session.sparkContext()).thenReturn(sparkContext);
  }

  @Test
  void testCTASCommand() {
    CreateDataSourceTableAsSelectCommandVisitor visitor =
        new CreateDataSourceTableAsSelectCommandVisitor(
            OpenLineageContext.builder()
                .sparkSession(session)
                .sparkContext(session.sparkContext())
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build());

    CreateDataSourceTableAsSelectCommand command =
        new CreateDataSourceTableAsSelectCommand(
            SparkUtils.catalogTable(
                TableIdentifier$.MODULE$.apply("tablename", Option.apply("db")),
                CatalogTableType.EXTERNAL(),
                CatalogStorageFormat$.MODULE$.apply(
                    Option.apply(URI.create("s3://bucket/directory")),
                    null,
                    null,
                    null,
                    false,
                    Map$.MODULE$.empty()),
                new StructType(
                    new StructField[] {
                      new StructField(
                          "key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
                      new StructField(
                          "value", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
                    })),
            null,
            mock(LogicalPlan.class),
            Seq$.MODULE$.<String>empty());

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(1, datasets.size());
    OpenLineage.OutputDataset outputDataset = datasets.get(0);

    assertEquals(
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE,
        outputDataset.getFacets().getLifecycleStateChange().getLifecycleStateChange());
    assertEquals("directory", outputDataset.getName());
    assertEquals("s3://bucket", outputDataset.getNamespace());
  }

  @Test
  void testJobNameSuffix() {
    CreateDataSourceTableAsSelectCommandVisitor visitor =
        new CreateDataSourceTableAsSelectCommandVisitor(mock(OpenLineageContext.class));
    CreateDataSourceTableAsSelectCommand command = mock(CreateDataSourceTableAsSelectCommand.class);

    CatalogTable catalogTable = mock(CatalogTable.class);
    when(command.table()).thenReturn(catalogTable);
    when(catalogTable.identifier()).thenReturn(new TableIdentifier("table", Option.apply("db")));

    assertThat(visitor.jobNameSuffix(command)).isPresent().get().isEqualTo("db_table");
  }
}
