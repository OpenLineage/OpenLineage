/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark.agent.lifecycle.plan.column.ColumnLevelLineageVisitor;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import scala.PartialFunction;

/**
 * Provides input and output dataset builders
 *
 * <p>All common {@link AbstractQueryPlanDatasetBuilder} need to be grouped and passed into {@link
 * DatasetBuilderFactory#getInputBuilders(OpenLineageContext)} or {@link
 * DatasetBuilderFactory#getOutputBuilders(OpenLineageContext)} in order to produce within
 * OpenLineage event {@link OpenLineage.InputDataset} and {@link OpenLineage.OutputDataset}
 * respectively.
 */
public interface DatasetBuilderFactory {

  /**
   * No-op factory used for contexts not built via the agent's ContextFactory, such as some unit
   * tests
   */
  DatasetBuilderFactory EMPTY =
      new DatasetBuilderFactory() {
        @Override
        public Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
            OpenLineageContext context) {
          return Collections.emptyList();
        }

        @Override
        public Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
            getOutputBuilders(OpenLineageContext context) {
          return Collections.emptyList();
        }
      };

  Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
      OpenLineageContext context);

  Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
      OpenLineageContext context);

  default Collection<ColumnLevelLineageVisitor> getColumnLevelLineageVisitors(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Catalog handlers contributed by this Spark version. Lets version-specific modules (e.g. Spark 4
   * for open source Unity Catalog) add their {@link CatalogHandler}s to the shared catalog
   * resolution flow without lower modules depending on them.
   */
  default List<CatalogHandler> getCatalogHandlers(OpenLineageContext context) {
    return Collections.emptyList();
  }
}
