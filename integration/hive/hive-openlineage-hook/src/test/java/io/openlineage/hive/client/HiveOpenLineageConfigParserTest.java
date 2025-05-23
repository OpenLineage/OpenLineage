/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.client;

import static io.openlineage.hive.client.HiveOpenLineageConfigParser.extractFromHadoopConf;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.transports.ConsoleConfig;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class HiveOpenLineageConfigParserTest {

  @Test
  public void testEmptyConfig() {
    Configuration conf = new Configuration();
    HiveOpenLineageConfig config = extractFromHadoopConf(conf);
    assertThat(config.getTransportConfig()).isNull();
    assertThat(config.getJobConfig()).isNull();
    assertThat(config.getFacetsConfig()).isNull();
    assertThat(config.getCircuitBreaker()).isNull();
    assertThat(config.getMetricsConfig()).isNull();
    assertThat(config.getDatasetConfig()).isNull();
  }

  @Test
  public void testSimpleTransportConfig() {
    Configuration conf = new Configuration();
    conf.set("hive.openlineage.transport.type", "console");
    HiveOpenLineageConfig config = extractFromHadoopConf(conf);
    assertThat(config.getTransportConfig()).isInstanceOf(ConsoleConfig.class);
    assertThat(config.getJobConfig()).isNull();
    assertThat(config.getFacetsConfig()).isNull();
    assertThat(config.getCircuitBreaker()).isNull();
    assertThat(config.getMetricsConfig()).isNull();
    assertThat(config.getDatasetConfig()).isNull();
  }

}
