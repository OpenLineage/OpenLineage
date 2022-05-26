package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.api.AbstractQueryPlanDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.Collection;
import java.util.List;
import scala.PartialFunction;

/**
 * Provides input and output dataset builders
 *
 * <p>All common {@link AbstractQueryPlanDatasetBuilder} need to be grouped and passed into {@link
 * DatasetBuilderFactory#getInputBuilders(io.openlineage.spark.api.OpenLineageContext)} or {@link
 * DatasetBuilderFactory#getOutputBuilders(io.openlineage.spark.api.OpenLineageContext)} in order to
 * produce within OpenLineage event {@link OpenLineage.InputDataset} and {@link
 * OpenLineage.OutputDataset} respectively.
 */
public interface DatasetBuilderFactory {

  Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>> getInputBuilders(
      OpenLineageContext context);

  Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>> getOutputBuilders(
      OpenLineageContext context);
}
