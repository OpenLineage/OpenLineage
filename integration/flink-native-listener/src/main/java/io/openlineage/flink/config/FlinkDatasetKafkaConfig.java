/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import lombok.Getter;
import lombok.Setter;

public class FlinkDatasetKafkaConfig implements MergeConfig<FlinkDatasetKafkaConfig> {

  @JsonProperty("resolveTopicPattern")
  @Getter
  @Setter
  private boolean resolveTopicPattern;

  @Override
  public FlinkDatasetKafkaConfig mergeWithNonNull(FlinkDatasetKafkaConfig flinkDatasetKafkaConfig) {
    return null;
  }
}
