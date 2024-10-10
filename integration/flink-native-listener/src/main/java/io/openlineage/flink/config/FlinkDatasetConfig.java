/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
@EqualsAndHashCode(callSuper = true)
@AllArgsConstructor
public class FlinkDatasetConfig extends DatasetConfig {

  @JsonProperty("resolveTopicPattern")
  @Getter
  @Setter
  private boolean resolveTopicPattern;

  public FlinkDatasetConfig(
      boolean resolveTopicPattern, Map<String, DatasetNamespaceResolverConfig> namespaceResolvers) {
    super(namespaceResolvers);
    this.resolveTopicPattern = resolveTopicPattern;
  }

  @Override
  public DatasetConfig mergeWithNonNull(DatasetConfig t) {
    if (t instanceof FlinkDatasetConfig) {
      return new FlinkDatasetConfig(
          resolveTopicPattern && ((FlinkDatasetConfig) t).resolveTopicPattern,
          mergePropertyWith(getNamespaceResolvers(), t.getNamespaceResolvers()));
    } else {
      return super.mergeWithNonNull(t);
    }
  }
}
