/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

public class PatternNamespaceResolverBuilder implements DatasetNamespaceResolverBuilder {

  @Override
  public String getType() {
    return "pattern";
  }

  @Override
  public DatasetNamespaceResolverConfig getConfig() {
    return new PatternNamespaceResolverConfig();
  }

  @Override
  public PatternNamespaceResolver build(String name, DatasetNamespaceResolverConfig config) {
    return new PatternNamespaceResolver(name, (PatternNamespaceResolverConfig) config);
  }
}
