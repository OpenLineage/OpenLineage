package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/** Factory for the builders that generate OpenLineage components and facets from Spark events. */
public interface OpenLineageEventHandlerFactory {

  default List<PartialFunction<LogicalPlan, List<InputDataset>>>
      createInputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<PartialFunction<LogicalPlan, List<OutputDataset>>>
      createOutputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<PartialFunction<Object, List<InputDataset>>> createInputDatasetBuilder(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<PartialFunction<Object, List<OutputDataset>>> createOutputDatasetBuilder(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<CustomFacetBuilder<?, ? extends InputDatasetFacet>> createInputDatasetFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>
      createOutputDatasetFacetBuilders(OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<CustomFacetBuilder<?, ? extends DatasetFacet>> createDatasetFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<CustomFacetBuilder<?, ? extends RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  default List<CustomFacetBuilder<?, ? extends JobFacet>> createJobFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }
}
