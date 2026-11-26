/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.spark.api.OpenLineageContext;
import java.util.Optional;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;
import scala.Option;

class FabricEventHandlerFactoryTest {
  private final FabricEventHandlerFactory factory = new FabricEventHandlerFactory();
  private final OpenLineageContext context = mock(OpenLineageContext.class);

  @Test
  void testNamesNothingOutsideFabric() {
    SparkSession session = mock(SparkSession.class);
    SparkContext sparkContext = mock(SparkContext.class);
    RuntimeConfig conf = mock(RuntimeConfig.class);
    when(conf.getOption(org.mockito.ArgumentMatchers.anyString())).thenReturn(Option.empty());
    when(session.conf()).thenReturn(conf);
    when(session.sparkContext()).thenReturn(sparkContext);
    when(sparkContext.getConf()).thenReturn(new SparkConf(false));
    when(sparkContext.hadoopConfiguration()).thenReturn(new Configuration(false));
    when(context.getSparkSession()).thenReturn(Optional.of(session));

    assertThat(factory.createDatasetIdentifierResolvers(context)).isEmpty();
  }

  @Test
  void testNamesNothingWithoutASession() {
    when(context.getSparkSession()).thenReturn(Optional.empty());

    assertThat(factory.createDatasetIdentifierResolvers(context)).isEmpty();
  }

  @Test
  void testNamesNothingWhenTheSettingsCantBeRead() {
    // for example a mocked session in another vendor's tests, which has no settings
    when(context.getSparkSession()).thenReturn(Optional.of(mock(SparkSession.class)));

    assertThat(factory.createDatasetIdentifierResolvers(context)).isEmpty();
  }
}
