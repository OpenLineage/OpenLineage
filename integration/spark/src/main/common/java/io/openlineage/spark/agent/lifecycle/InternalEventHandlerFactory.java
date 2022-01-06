package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

class InternalEventHandlerFactory implements OpenLineageEventHandlerFactory {
  private final List<PartialFunction<LogicalPlan, List<InputDataset>>>
      inputDatasetQueryPlanVisitors;
  private final List<PartialFunction<LogicalPlan, List<OutputDataset>>>
      outputDatasetQueryPlanVisitors;
  private final List<PartialFunction<Object, List<InputDataset>>> inputDatasetBuilder;
  private final List<CustomFacetBuilder<?, ? extends InputDatasetFacet>> inputDatasetFacetBuilders;
  private final List<PartialFunction<Object, List<OutputDataset>>> outputDatasetBuilder;
  private final List<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>
      outputDatasetFacetBuilders;
  private final List<CustomFacetBuilder<?, ? extends DatasetFacet>> datasetFacetBuilders;
  private final List<CustomFacetBuilder<?, ? extends JobFacet>> jobFacetBuilders;
  private final List<CustomFacetBuilder<?, ? extends RunFacet>> runFacetBuilders;

  public InternalEventHandlerFactory(OpenLineageContext context) {
    ServiceLoader<OpenLineageEventHandlerFactory> loader = ServiceLoader.load(
        OpenLineageEventHandlerFactory.class);
    this.inputDatasetQueryPlanVisitors =
        generate(loader, factory -> factory.createInputDatasetQueryPlanVisitors(context));
    this.outputDatasetQueryPlanVisitors =
        generate(loader, factory -> factory.createOutputDatasetQueryPlanVisitors(context));
    this.inputDatasetBuilder =
        generate(loader, factory -> factory.createInputDatasetBuilder(context));
    this.outputDatasetBuilder =
        generate(loader, factory -> factory.createOutputDatasetBuilder(context));
    this.inputDatasetFacetBuilders =
        generate(loader, factory -> factory.createInputDatasetFacetBuilders(context));
    this.outputDatasetFacetBuilders =
        generate(loader, factory -> factory.createOutputDatasetFacetBuilders(context));
    this.datasetFacetBuilders =
        generate(loader, factory -> factory.createDatasetFacetBuilders(context));
    this.jobFacetBuilders =
        generate(loader, factory -> factory.createJobFacetBuilders(context));
    this.runFacetBuilders =
        generate(loader, factory -> factory.createRunFacetBuilders((context)));
  }

  /**
   * Invoke a method on each of the supplied {@link OpenLineageEventHandlerFactory}s and merge the
   * results into a single list.
   * @param factories
   * @param supplier
   * @param <T>
   * @return
   */
  private <T> List<T> generate(ServiceLoader<OpenLineageEventHandlerFactory> factories,
      Function<OpenLineageEventHandlerFactory, List<T>> supplier) {
    return StreamSupport.stream(
            Spliterators.spliteratorUnknownSize(factories.iterator(), Spliterator.IMMUTABLE), false)
        .flatMap(supplier.andThen(List::stream))
        .collect(Collectors.toList());
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<InputDataset>>> createInputDatasetQueryPlanVisitors(
      OpenLineageContext context) {
    return inputDatasetQueryPlanVisitors;
  }

  @Override
  public List<PartialFunction<LogicalPlan, List<OutputDataset>>> createOutputDatasetQueryPlanVisitors(
      OpenLineageContext context) {
    return outputDatasetQueryPlanVisitors;
  }

  @Override
  public List<PartialFunction<Object, List<InputDataset>>> createInputDatasetBuilder(
      OpenLineageContext context) {
    return inputDatasetBuilder;
  }

  @Override
  public List<PartialFunction<Object, List<OutputDataset>>> createOutputDatasetBuilder(
      OpenLineageContext context) {
    return outputDatasetBuilder;
  }

  @Override
  public List<CustomFacetBuilder<?, ? extends InputDatasetFacet>> createInputDatasetFacetBuilders(
      OpenLineageContext context) {
    return inputDatasetFacetBuilders;
  }

  @Override
  public List<CustomFacetBuilder<?, ? extends OutputDatasetFacet>> createOutputDatasetFacetBuilders(
      OpenLineageContext context) {
    return outputDatasetFacetBuilders;
  }

  @Override
  public List<CustomFacetBuilder<?, ? extends DatasetFacet>> createDatasetFacetBuilders(
      OpenLineageContext context) {
    return datasetFacetBuilders;
  }

  @Override
  public List<CustomFacetBuilder<?, ? extends RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return runFacetBuilders;
  }

  @Override
  public List<CustomFacetBuilder<?, ? extends JobFacet>> createJobFacetBuilders(OpenLineageContext context) {
    return jobFacetBuilders;
  }
}
