/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

public interface DatasetNamespaceResolverBuilder {

  String getType();

  DatasetNamespaceResolverConfig getConfig();

  DatasetNamespaceResolver build(String name, DatasetNamespaceResolverConfig config);
}
