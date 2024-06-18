/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

public class HostListNamespaceResolverBuilder implements DatasetNamespaceResolverBuilder {

  @Override
  public String getType() {
    return "hostList";
  }

  @Override
  public DatasetNamespaceResolverConfig getConfig() {
    return new HostListNamespaceResolverConfig();
  }

  @Override
  public HostListNamespaceResolver build(String name, DatasetNamespaceResolverConfig config) {
    return new HostListNamespaceResolver(name, (HostListNamespaceResolverConfig) config);
  }
}
