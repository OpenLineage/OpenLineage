/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Setter
@NoArgsConstructor
@ToString
@Getter
@AllArgsConstructor
public class FlinkDatasetKafkaConfig implements MergeConfig<FlinkDatasetKafkaConfig> {

  @JsonProperty("resolveTopicPattern")
  @Setter
  private Boolean resolveTopicPattern;

  @Override
  public FlinkDatasetKafkaConfig mergeWithNonNull(FlinkDatasetKafkaConfig other) {
    return new FlinkDatasetKafkaConfig(
        mergeWithDefaultValue(resolveTopicPattern, other.resolveTopicPattern, Boolean.TRUE));
  }

  public Boolean getResolveTopicPattern() {
    return Optional.ofNullable(resolveTopicPattern).orElse(Boolean.TRUE);
  }
}
