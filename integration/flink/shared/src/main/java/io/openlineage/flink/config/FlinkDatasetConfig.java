/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceResolverConfig;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Setter
@ToString
@Getter
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
public class FlinkDatasetConfig extends DatasetConfig {

  @JsonProperty("kafka")
  private FlinkDatasetKafkaConfig kafkaConfig;

  public FlinkDatasetConfig() {
    super();
    this.kafkaConfig = new FlinkDatasetKafkaConfig();
  }

  public FlinkDatasetConfig(
      FlinkDatasetKafkaConfig kafkaConfig,
      Map<String, DatasetNamespaceResolverConfig> namespaceResolvers,
      String disabledTrimmers,
      String extraTrimmers) {
    super(namespaceResolvers, disabledTrimmers, extraTrimmers);
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public DatasetConfig mergeWithNonNull(DatasetConfig t) {
    if (t instanceof FlinkDatasetConfig) {
      return new FlinkDatasetConfig(
          mergePropertyWith(kafkaConfig, ((FlinkDatasetConfig) t).kafkaConfig),
          mergePropertyWith(getNamespaceResolvers(), t.getNamespaceResolvers()),
          mergePropertyWith(getDisabledTrimmers(), t.getDisabledTrimmers()),
          mergePropertyWith(getExtraTrimmers(), t.getExtraTrimmers()));
    } else {
      return super.mergeWithNonNull(t);
    }
  }
}
