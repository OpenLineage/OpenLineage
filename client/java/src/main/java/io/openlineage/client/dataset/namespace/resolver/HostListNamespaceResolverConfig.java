/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import io.openlineage.client.MergeConfig;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class HostListNamespaceResolverConfig
    implements DatasetNamespaceResolverConfig, MergeConfig<HostListNamespaceResolverConfig> {

  @NonNull @Getter @Setter private List<String> hosts = null;
  @Getter @Setter private String schema;

  @Override
  public HostListNamespaceResolverConfig mergeWithNonNull(HostListNamespaceResolverConfig t) {
    return new HostListNamespaceResolverConfig(
        mergePropertyWith(hosts, t.getHosts()), mergePropertyWith(schema, t.schema));
  }
}
