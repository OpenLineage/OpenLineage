/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.AbstractQueryPlanOutputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.Join;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.MapPartitions;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import scala.runtime.BoxedUnit;

@Slf4j
public class MapPartitionsDatasetBuilder
    extends AbstractQueryPlanOutputDatasetBuilder<MapPartitions> {

  public MapPartitionsDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof MapPartitions;
  }

  @Override
  protected List<OutputDataset> apply(SparkListenerEvent event, MapPartitions x) {
    if (x.func().toString().contains("org.apache.spark.sql.delta.commands.MergeIntoCommand")) {
      return applyForMergeInto(event, x);
    }

    log.debug("MapPartitionsDatasetBuilder apply works only with MergeIntoCommand command on 3.1");
    return Collections.emptyList();
  }

  /**
   * In case of delta MergeIntoCommand for Spark 3.1, the command is translated into following plan:
   *
   * <pre>
   * == Optimized Logical Plan ==
   * SerializeFromObject ...
   * +- MapPartitions org.apache.spark.sql.delta.commands.MergeIntoCommand$$Lambda$ ...
   *   +- Join FullOuter, (a#815L = a#819L)
   *        :- Project [a#819L, b#820, UDF() AS _source_row_present_#1052]
   *          : +- Relation[a#819L,b#820] parquet
   *        +- Project [a#815L, b#816, true AS _target_row_present_#1058]
   *            +- Relation[a#815L,b#816] parquet
   * </pre>
   *
   * The code below checks if this is the case. It checks if there is a full outer join with a
   * joined projection containing field `_target_row_present_`. If so, it extracts output dataset
   * from that projection.
   */
  protected List<OutputDataset> applyForMergeInto(SparkListenerEvent event, MapPartitions x) {
    List<OutputDataset> datasets = new ArrayList<>();
    context
        .getQueryExecution()
        .ifPresent(
            qe ->
                qe.optimizedPlan()
                    .foreach(
                        node -> {
                          Optional.of(node)
                              .filter(n -> n instanceof Join)
                              .map(Join.class::cast)
                              .filter(
                                  join ->
                                      join.joinType()
                                          .toString()
                                          .equalsIgnoreCase(JoinType.FULLOUTER.toString()))
                              .filter(join -> join.children() != null)
                              .map(join -> join.children().last())
                              .filter(n -> n instanceof Project)
                              .map(Project.class::cast)
                              .filter(p -> p.output() != null && p.output().nonEmpty())
                              .filter(p -> p.output().last().name().startsWith("_target_row"))
                              .ifPresent(
                                  join -> {
                                    datasets.addAll(
                                        delegate(
                                                context.getOutputDatasetQueryPlanVisitors(),
                                                context.getOutputDatasetBuilders(),
                                                event)
                                            .applyOrElse(
                                                join.children().last(),
                                                ScalaConversionUtils.toScalaFn(
                                                    (lp) -> Collections.<OutputDataset>emptyList()))
                                            .stream()
                                            .collect(Collectors.toList()));
                                  });
                          return BoxedUnit.UNIT;
                        }));

    return datasets;
  }
}
