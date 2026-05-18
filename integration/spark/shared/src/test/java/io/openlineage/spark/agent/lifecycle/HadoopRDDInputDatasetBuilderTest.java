/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.net.URI;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.junit.jupiter.api.Test;

class HadoopRDDInputDatasetBuilderTest {
  @Test
  void findInputsNormalizesInputPathsToDirectoriesAndRemovesGlobs() {
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration());

    RDD<?> rdd = mock(RDD.class);
    when(rdd.sparkContext()).thenReturn(sparkContext);

    HadoopRDDInputDatasetBuilder builder =
        new HadoopRDDInputDatasetBuilder(mock(OpenLineageContext.class)) {
          @Override
          protected Path[] getInputPaths(RDD<?> rdd) {
            return new Path[] {new Path("s3://bucket/table/dt=20260516/*.parquet")};
          }
        };

    List<URI> inputs = builder.findInputs(rdd);

    assertThat(inputs).containsExactly(URI.create("s3://bucket/table/dt=20260516"));
  }
}
