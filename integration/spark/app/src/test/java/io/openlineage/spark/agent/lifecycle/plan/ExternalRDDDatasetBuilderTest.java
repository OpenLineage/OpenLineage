/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Spark4CompatUtils;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.nio.file.Path;
import java.util.List;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExternalRDD;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
class ExternalRDDDatasetBuilderTest {

  @AfterEach
  public void tearDown() {
    Spark4CompatUtils.cleanupAnyExistingSession();
  }

  @Test
  void testFileReadByExternalRddIsAnInput(@TempDir Path tmpDir) {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    ExternalRDDDatasetBuilder builder =
        new ExternalRDDDatasetBuilder(SparkAgentTestExtension.newContext(session));
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("value", StringType$.MODULE$, true, new Metadata(new HashMap<>()))
            });
    RDD<String> textRdd = session.sparkContext().textFile("file://" + tmpDir, 1);

    ExternalRDD<?> externalRDD = mock(ExternalRDD.class);
    when(externalRDD.rdd()).thenReturn((RDD) textRdd);
    when(externalRDD.schema()).thenReturn(schema);

    assertThat(builder.isDefinedAtLogicalPlan(externalRDD)).isTrue();
    List<OpenLineage.InputDataset> datasets = builder.apply(externalRDD);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", tmpDir.toString())
        .hasFieldOrPropertyWithValue("namespace", "file");
    assertThat(datasets.get(0).getFacets().getSchema().getFields())
        .extracting(OpenLineage.SchemaDatasetFacetFields::getName)
        .containsExactly("value");
  }
}
