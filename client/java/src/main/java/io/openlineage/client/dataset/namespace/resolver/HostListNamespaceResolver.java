/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import org.apache.commons.lang3.StringUtils;

public class HostListNamespaceResolver implements DatasetNamespaceResolver {

  private final String resolvedName;
  private final HostListNamespaceResolverConfig config;

  public HostListNamespaceResolver(String resolvedName, HostListNamespaceResolverConfig config) {
    this.resolvedName = resolvedName;
    this.config = config;
  }

  @Override
  public String resolve(String namespace) {
    if (config.getHosts() == null) {
      return namespace;
    }

    if (StringUtils.isNotEmpty(config.getSchema())
        && !namespace.startsWith(config.getSchema() + "://")) {
      // schema configured but is not matching
      return namespace;
    }

    return config.getHosts().stream()
        .filter(h -> StringUtils.containsIgnoreCase(namespace, h))
        .findAny()
        .map(h -> StringUtils.replaceIgnoreCase(namespace, h, resolvedName))
        .orElseGet(() -> namespace);
  }
}
