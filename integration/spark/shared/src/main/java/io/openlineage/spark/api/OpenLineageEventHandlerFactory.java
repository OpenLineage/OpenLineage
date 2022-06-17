/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
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
 *   <li>{@link StageInfo}
 * </ul>
 *
 * The {@link PartialFunction#isDefinedAt(Object)} method should define which types the functions
 * are defined for. The function should be defined for the most specific type that is needed for the
 * function's logic to complete. For example, if the function requires a {@link
 * org.apache.spark.scheduler.SparkListenerJobEnd}, it should be defined for that type, not the
 * generic {@link org.apache.spark.scheduler.SparkListenerEvent} type.
 *
 * <p>This interface is evolving and may change in future releases.
 *
 * @apiNote This interface is evolving and may change in future releases
 */
public interface OpenLineageEventHandlerFactory {

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to a single {@link
   * LogicalPlan} node to construct an {@link OpenLineage.InputDataset}. {@link PartialFunction}
   * implementations will typically extend {@link QueryPlanVisitor}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>>
      createInputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to a single {@link
   * LogicalPlan} node to construct an {@link OpenLineage.OutputDataset}. {@link PartialFunction}
   * implementations will typically extend {@link QueryPlanVisitor}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>>
      createOutputDatasetQueryPlanVisitors(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link OpenLineage.InputDataset}. Typically, implementations should extend {@link
   * AbstractInputDatasetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<Object, List<OpenLineage.InputDataset>>>
      createInputDatasetBuilder(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link OpenLineage.OutputDataset}. Typically, implementations should extend {@link
   * AbstractOutputDatasetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<PartialFunction<Object, List<OpenLineage.OutputDataset>>>
      createOutputDatasetBuilder(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link OpenLineage.InputDatasetFacet}s. {@link OpenLineage.InputDatasetFacet}s
   * created by these functions may be attached to <i>any</i> {@link OpenLineage.InputDataset}
   * defined for the current context - typically, there is only one {@link OpenLineage.InputDataset}
   * defined for a single Spark {@link org.apache.spark.scheduler.Stage}. {@link PartialFunction}s
   * defined for a {@link org.apache.spark.scheduler.Stage} or {@link StageInfo} can be matched to a
   * specific {@link OpenLineage.InputDataset} if the required facet information is available. For
   * example, a facet detailing the number of records read for an {@link OpenLineage.InputDataset}
   * can be constructed from the {@link StageInfo#taskMetrics()} returned by the stage where that
   * dataset is read. If a user wants to ensure the {@link OpenLineage.InputDatasetFacet} is
   * targeted to a specific {@link OpenLineage.InputDataset}, they should implement a builder that
   * returns an {@link OpenLineage.InputDataset} and directly attach the facet. Merging the dataset
   * and its facets can be handled separately. Typically, implementations should extend {@link
   * AbstractInputDatasetFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends OpenLineage.InputDatasetFacet>>
      createInputDatasetFacetBuilders(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link OpenLineage.OutputDatasetFacet}s. {@link OpenLineage.OutputDatasetFacet}s
   * created by these functions may be attached to <i>any</i> {@link OpenLineage.OutputDataset}
   * defined for the current context - typically, there is only one {@link
   * OpenLineage.OutputDataset} defined for a given Spark application. If a user wants to ensure the
   * {@link OpenLineage.OutputDatasetFacet} is targeted to a specific {@link
   * OpenLineage.OutputDataset}, they should implement a builder that returns an {@link
   * OpenLineage.OutputDataset} and directly attach the facet. Merging the dataset and its facets
   * can be handled separately. Typically, implementations should extend {@link
   * AbstractOutputDatasetFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends OpenLineage.OutputDatasetFacet>>
      createOutputDatasetFacetBuilders(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct an {@link OpenLineage.DatasetFacet}s. {@link OpenLineage.DatasetFacet}s created by
   * these functions may be attached to <i>any</i> {@link io.openlineage.client.OpenLineage.Dataset}
   * defined for the current context - this will include any {@link OpenLineage.InputDataset} or
   * {@link OpenLineage.OutputDataset} found in the current context. If a user wants to ensure the
   * {@link OpenLineage.DatasetFacet} is targeted to a specific {@link OpenLineage.OutputDataset} or
   * {@link OpenLineage.InputDataset}, they should implement a builder that returns an {@link
   * io.openlineage.client.OpenLineage.Dataset} and directly attach the facet. Merging the dataset
   * and its facets can be handled separately. Typically, implementations should extend {@link
   * AbstractDatasetFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends OpenLineage.DatasetFacet>>
      createDatasetFacetBuilders(OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct a {@link OpenLineage.RunFacet} which will be attached to the current {@link
   * io.openlineage.client.OpenLineage.RunEvent} Typically, implementations should extend {@link
   * AbstractRunFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends OpenLineage.RunFacet>> createRunFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }

  /**
   * Create a collection of {@link PartialFunction}s that may be applied to various Spark
   * application events and other objects (see class javadocs) that can contain information used to
   * construct a {@link OpenLineage.JobFacet} which will be attached to the current {@link
   * io.openlineage.client.OpenLineage.Job}. Typically, implementations should extend {@link
   * AbstractJobFacetBuilder}.
   *
   * @param context
   * @return
   */
  default Collection<CustomFacetBuilder<?, ? extends OpenLineage.JobFacet>> createJobFacetBuilders(
      OpenLineageContext context) {
    return Collections.emptyList();
  }
}
