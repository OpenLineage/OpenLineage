/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor;

import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.visitor.facet.DatasetFacetVisitor;
import io.openlineage.flink.visitor.identifier.DatasetIdentifierVisitor;
import java.util.Collection;
import org.apache.flink.streaming.api.lineage.LineageDataset;

/**
 * Factory to load all the {@link VisitorFactory} to be applied on facets returned within Flink's
 * lineage graph. There two types of visitors: {@link DatasetIdentifierVisitor} and {@link
 * DatasetFacetVisitor}.
 *
 * <p>{@link DatasetIdentifierVisitor} allow extracting OpenLineage dataset identifiers from Flink's
 * {@link LineageDataset}. They allow exploding datasets. In this case a single {@link
 * LineageDataset} gets multiple OpenLineage dataset identifiers and this will result in multiple
 * OpenLineage datasets having all the facets in common but a different name. This pattern is useful
 * to hand Flink's dataset with topic pattern.
 *
 * <p>{@link DatasetFacetVisitor} allows created OpenLineage dataset facets From Flink's {@link
 * LineageDataset}.
 */
public interface VisitorFactory {
  Collection<DatasetFacetVisitor> loadDatasetFacetVisitors(OpenLineageContext context);

  Collection<DatasetIdentifierVisitor> loadDatasetIdentifierVisitors(OpenLineageContext context);
}
