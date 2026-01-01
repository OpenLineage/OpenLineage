/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

public class PatternMatchingGroupNamespaceResolverBuilder
    implements DatasetNamespaceResolverBuilder {

  @Override
  public String getType() {
    return "patternGroup";
  }

  @Override
  public DatasetNamespaceResolverConfig getConfig() {
    return new PatternMatchingGroupNamespaceResolverConfig();
  }

  @Override
  public PatternMatchingGroupNamespaceResolver build(
      String name, DatasetNamespaceResolverConfig config) {
    return new PatternMatchingGroupNamespaceResolver(
        (PatternMatchingGroupNamespaceResolverConfig) config);
  }
}
