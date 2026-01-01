/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

/**
 * Interface used to resolve host name into more meaningful name that just host address. Common use
 * case for the interface is: resolve DB server into more descriptive name which is stable when
 * migrating hosts. Other use case is cluster matching: given any hostname from available in
 * cluster, a dataset identifier should be based on cluster name.
 */
public interface DatasetNamespaceResolver {

  /**
   * Method should always return original value if hostAdress is not to be modified
   *
   * @param namespace address of the host
   * @return resolved namespace
   */
  String resolve(String namespace);
}
