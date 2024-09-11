/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.flink.client.OpenLineageContext;
import java.util.Collection;

/**
 * Factory to load all the {@link VisitorFactory} to be applied on facets returned within Flink's
 * lineage graph.
 */
public interface VisitorFactory {
  Collection<DatasetFacetVisitor> loadDatasetFacetVisitors(OpenLineageContext context);

  Collection<DatasetIdentifierVisitor> loadDatasetIdentifierVisitors(OpenLineageContext context);
}
