/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import io.openlineage.client.OpenLineageConfig;
import io.openlineage.client.dataset.DatasetConfig;
import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;

/**
 * Utility class to resolve hosts based on the dataset host resolver configured. Methods of the
 * class should return original host address in case of no dataset host resolver defined
 */
public class DatasetNamespaceCombinedResolver {

  private final List<DatasetNamespaceResolver> resolvers;

  public DatasetNamespaceCombinedResolver(OpenLineageConfig config) {
    if (config == null || config.getDatasetConfig() == null) {
      this.resolvers = Collections.emptyList();
    } else {
      this.resolvers =
          DatasetNamespaceResolverLoader.loadDatasetNamespaceResolvers(config.getDatasetConfig());
    }
  }

  public DatasetNamespaceCombinedResolver(DatasetConfig config) {
    if (config == null) {
      this.resolvers = Collections.emptyList();
    } else {
      this.resolvers = DatasetNamespaceResolverLoader.loadDatasetNamespaceResolvers(config);
    }
  }

  /**
   * Resolves namespace by dataset host resolvers defined in dataset config. Whole namespace string
   * is passed to dataset namespace resolvers.
   *
   * @param namespace namespace that may contain host address to be resolved
   * @return resolved host address or the original one
   */
  public String resolve(String namespace) {
    for (DatasetNamespaceResolver resolver : resolvers) {
      String resolved = resolver.resolve(namespace);
      if (!resolved.equals(namespace)) {
        return resolved;
      }
    }
    return namespace;
  }

  public DatasetIdentifier resolve(DatasetIdentifier identifier) {
    return new DatasetIdentifier(
        identifier.getName(), resolve(identifier.getNamespace()), identifier.getSymlinks());
  }

  /**
   * Resolves namespace uri by dataset host resolvers defined in dataset config. Only host of the
   * URI is passed to dataset namespace resolvers configured.
   *
   * @param namespace host address to be resolved
   * @return resolved host address or the original one
   */
  public URI resolveHost(URI namespace) {
    String resolvedHost = this.resolve(namespace.getHost());

    if (!resolvedHost.equals(namespace.getHost())) {
      try {
        return new URI(
            namespace.getScheme(),
            namespace.getUserInfo(),
            resolvedHost,
            namespace.getPort(),
            namespace.getPath(),
            namespace.getQuery(),
            namespace.getFragment());
      } catch (URISyntaxException e) {
        return namespace;
      }
    }

    return namespace;
  }
}
