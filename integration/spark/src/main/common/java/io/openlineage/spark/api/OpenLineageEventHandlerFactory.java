package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import scala.PartialFunction;

/**
 * Factory for the builders that generate OpenLineage components and facets from Spark events.
 * Factories will be created once at the beginning of a Spark application (when the {@link
 * org.apache.spark.scheduler.SparkListenerApplicationStart} event fires). Each of the createXXX
 * methods will be invoked once per OpenLineage run - this may translate to a single {@link
 * org.apache.spark.sql.execution.QueryExecution} or a single {@link
 * org.apache.spark.scheduler.ActiveJob}. Each createXXX method is supplied with an {@link
 * OpenLineageContext}, which will contain references to the {@link org.apache.spark.SparkContext}
 * and other references that may be useful to builders that need access to information about the
 * Spark application.
 *
 * <p>The {@link #createInputDatasetQueryPlanVisitors(OpenLineageContext)} and {@link
 * #createOutputDatasetQueryPlanVisitors(OpenLineageContext)} methods return {@link
 * PartialFunction}s that are defined for {@link LogicalPlan} nodes. All other methods return {@link
 * PartialFunction}s or {@link CustomFacetBuilder}s (which mirror {@link PartialFunction}) that may
 * require other Spark application objects to construct the OpenLineage model objects. These other
 * types include
 *
 * <ul>
 *   <li>{@link org.apache.spark.scheduler.SparkListenerEvent}s
 *   <li>{@link org.apache.spark.rdd.RDD}
 *   <li>{@link org.apache.spark.scheduler.Stage}
 *   <li>{@link org.apache.spark.scheduler.StageInfo}
 * </ul>
 *
 * The {@link PartialFunction#isDefinedAt(Object)} method should define which types the functions
 * are defined for. The function should be defined for the most specific type that is needed for the
 * function's logic to complete. For example, if the function requires a {@link
 * org.apache.spark.scheduler.SparkListenerJobEnd}, it should be defined for that type, not the
 * generic {@link org.apache.spark.scheduler.SparkListenerEvent} type.
 */
public interface OpenLineageEventHandlerFactory {

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to a single {@link
   * LogicalPlan} node to construct an {@link InputDataset}. {@link PartialFunction} implementations
   * will typically extend {@link QueryPlanVisitor}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<LogicalPlan, List<InputDataset>>>
      createInputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to a single {@link
   * LogicalPlan} node to construct an {@link OutputDataset}. {@link PartialFunction}
   * implementations will typically extend {@link QueryPlanVisitor}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<LogicalPlan, List<OutputDataset>>>
      createOutputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link InputDataset}. Typically, implementations should extend {@link
   * AbstractInputDatasetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<Object, List<InputDataset>>> createInputDatasetBuilder(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link OutputDataset}. Typically, implementations should extend {@link
   * AbstractOutputDatasetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<Object, List<OutputDataset>>> createOutputDatasetBuilder(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link InputDatasetFacet}s. {@link InputDatasetFacet}s created by these functions
   * may be attached to <i>any</i> {@link InputDataset} defined for the current context - typically,
   * there is only one {@link InputDataset} defined for a single Spark {@link
   * org.apache.spark.scheduler.Stage}. {@link PartialFunction}s defined for a {@link
   * org.apache.spark.scheduler.Stage} or {@link org.apache.spark.scheduler.StageInfo} can be
   * matched to a specific {@link InputDataset} if the required facet information is available. For
   * example, a facet detailing the number of records read for an {@link InputDataset} can be
   * constructed from the {@link StageInfo#taskMetrics()} returned by the stage where that dataset
   * is read. If a user wants to ensure the {@link InputDatasetFacet} is targeted to a specific
   * {@link InputDataset}, they should implement a builder that returns an {@link InputDataset} and
   * directly attach the facet. Merging the dataset and its facets can be handled separately.
   * Typically, implementations should extend {@link AbstractInputDatasetFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends InputDatasetFacet>>
      createInputDatasetFacetBuilders(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link OutputDatasetFacet}s. {@link OutputDatasetFacet}s created by these
   * functions may be attached to <i>any</i> {@link OutputDataset} defined for the current context -
   * typically, there is only one {@link OutputDataset} defined for a given Spark application. If a
   * user wants to ensure the {@link OutputDatasetFacet} is targeted to a specific {@link
   * OutputDataset}, they should implement a builder that returns an {@link OutputDataset} and
   * directly attach the facet. Merging the dataset and its facets can be handled separately.
   * Typically, implementations should extend {@link AbstractOutputDatasetFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends OutputDatasetFacet>>
      createOutputDatasetFacetBuilders(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link DatasetFacet}s. {@link DatasetFacet}s created by these functions may be
   * attached to <i>any</i> {@link io.openlineage.client.OpenLineage.Dataset} defined for the
   * current context - this will include any {@link InputDataset} or {@link OutputDataset} found in
   * the current context. If a user wants to ensure the {@link DatasetFacet} is targeted to a
   * specific {@link OutputDataset} or {@link InputDataset}, they should implement a builder that
   * returns an {@link io.openlineage.client.OpenLineage.Dataset} and directly attach the facet.
   * Merging the dataset and its facets can be handled separately. Typically, implementations should
   * extend {@link AbstractDatasetFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends DatasetFacet>> createDatasetFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct a {@link RunFacet} which will be attached to the current {@link
   * io.openlineage.client.OpenLineage.RunEvent} Typically, implementations should extend {@link
   * AbstractRunFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct a {@link JobFacet} which will be attached to the current {@link
   * io.openlineage.client.OpenLineage.Job}. Typically, implementations should extend {@link
   * AbstractJobFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends JobFacet>> createJobFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }
}
