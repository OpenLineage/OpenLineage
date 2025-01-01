/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import io.openlineage.client.dataset.namespace.resolver.DatasetNamespaceResolverConfig;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@ToString
public class DatasetConfig implements MergeConfig<DatasetConfig> {

  @Getter
  @Setter
  @JsonProperty("namespaceResolvers")
  private Map<String, DatasetNamespaceResolverConfig> namespaceResolvers;

  @Override
  public DatasetConfig mergeWithNonNull(DatasetConfig other) {
    return new DatasetConfig(mergePropertyWith(namespaceResolvers, other.namespaceResolvers));
  }
}
