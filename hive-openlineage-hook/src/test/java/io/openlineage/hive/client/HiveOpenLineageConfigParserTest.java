/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.client;

import static io.openlineage.hive.client.HiveOpenLineageConfigParser.extractFromHadoopConf;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.transports.ConsoleConfig;
import io.openlineage.client.transports.gcplineage.GcpLineageTransportConfig;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

public class HiveOpenLineageConfigParserTest {

  @Test
  public void testEmptyConfig() {
    Configuration conf = new Configuration();
    HiveOpenLineageConfig config = extractFromHadoopConf(conf);
    assertThat(config.getTransportConfig()).isNull();
    assertThat(config.getJob()).isNull();
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
    assertThat(config.getJob()).isNull();
    assertThat(config.getFacetsConfig()).isNull();
    assertThat(config.getCircuitBreaker()).isNull();
    assertThat(config.getMetricsConfig()).isNull();
    assertThat(config.getDatasetConfig()).isNull();
  }

  @Test
  public void testDataplexTransportConfig() {
    Configuration conf = new Configuration();
    conf.set("hive.openlineage.transport.type", "gcplineage");
    conf.set("hive.openlineage.transport.projectId", "myproject");
    conf.set("hive.openlineage.transport.location", "mylocation");
    HiveOpenLineageConfig config = extractFromHadoopConf(conf);
    assertThat(((GcpLineageTransportConfig) config.getTransportConfig()).getProjectId())
        .isEqualTo("myproject");
    assertThat(((GcpLineageTransportConfig) config.getTransportConfig()).getLocation())
        .isEqualTo("mylocation");
    assertThat(config.getJob()).isNull();
    assertThat(config.getFacetsConfig()).isNull();
    assertThat(config.getCircuitBreaker()).isNull();
    assertThat(config.getMetricsConfig()).isNull();
    assertThat(config.getDatasetConfig()).isNull();
  }
}
