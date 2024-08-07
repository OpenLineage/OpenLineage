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
import io.openlineage.spark.agent.lifecycle.plan.OptimizedCreateHiveTableAsSelectCommandVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Map$;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

class OptimizedCreateHiveTableAsSelectCommandVisitorTest {

  private static final String KEY = "key";
  private static final String VALUE = "value";
  SparkSession session = mock(SparkSession.class);

  @BeforeEach
  public void setUp() {
    SparkContext sparkContext = mock(SparkContext.class);
    SparkConf conf = new SparkConf();
    conf.set("spark.sql.warehouse.dir", "s3://bucket/warehouse/location");
    when(sparkContext.getConf()).thenReturn(conf);
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());
    when(session.sparkContext()).thenReturn(sparkContext);
  }

  @Test
  void testOptimizedCreateHiveTableAsSelectCommand() {
    OptimizedCreateHiveTableAsSelectCommandVisitor visitor =
        new OptimizedCreateHiveTableAsSelectCommandVisitor(
            OpenLineageContext.builder()
                .sparkSession(session)
                .sparkContext(session.sparkContext())
                .openLineage(new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI))
                .meterRegistry(new SimpleMeterRegistry())
                .openLineageConfig(new SparkOpenLineageConfig())
                .build());

    OptimizedCreateHiveTableAsSelectCommand command =
        new OptimizedCreateHiveTableAsSelectCommand(
            SparkUtils.catalogTable(
                TableIdentifier$.MODULE$.apply("tablename", Option.<String>apply("database")),
                CatalogTableType.EXTERNAL(),
                CatalogStorageFormat$.MODULE$.apply(
                    Option.<URI>apply(
                        URI.create("s3://bucket/warehouse/location/database.db/tablename")),
                    null,
                    null,
                    null,
                    false,
                    Map$.MODULE$.empty()),
                new StructType(
                    new StructField[] {
                      new StructField(
                          KEY,
                          IntegerType$.MODULE$,
                          false,
                          new Metadata(new HashMap<String, Object>())),
                      new StructField(
                          VALUE,
                          StringType$.MODULE$,
                          false,
                          new Metadata(new HashMap<String, Object>()))
                    })),
            new LogicalRelation(
                new JDBCRelation(
                    new StructType(
                        new StructField[] {
                          new StructField(KEY, IntegerType$.MODULE$, false, null),
                          new StructField(VALUE, StringType$.MODULE$, false, null)
                        }),
                    new Partition[] {},
                    new JDBCOptions(
                        "",
                        "temp",
                        ScalaUtils.<String, String>fromTuples(
                            Collections.singletonList(
                                Tuple2.<String, String>apply("driver", Driver.class.getName())))),
                    session),
                Seq$.MODULE$
                    .<AttributeReference>newBuilder()
                    .$plus$eq(
                        new AttributeReference(
                            KEY,
                            IntegerType$.MODULE$,
                            false,
                            null,
                            ExprId.apply(1L),
                            Seq$.MODULE$.<String>empty()))
                    .$plus$eq(
                        new AttributeReference(
                            VALUE,
                            StringType$.MODULE$,
                            false,
                            null,
                            ExprId.apply(2L),
                            Seq$.MODULE$.<String>empty()))
                    .result(),
                Option.<CatalogTable>empty(),
                false),
            ScalaConversionUtils.<String>fromList(Arrays.asList(KEY, VALUE)),
            SaveMode.Overwrite);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(1, datasets.size());
    OpenLineage.OutputDataset outputDataset = datasets.get(0);

    assertEquals(
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
        outputDataset.getFacets().getLifecycleStateChange().getLifecycleStateChange());
    assertEquals("warehouse/location/database.db/tablename", outputDataset.getName());
    assertEquals("s3://bucket", outputDataset.getNamespace());
  }
}
