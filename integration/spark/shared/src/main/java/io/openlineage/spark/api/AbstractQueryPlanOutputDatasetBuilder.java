/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;

/**
 * {@link AbstractQueryPlanDatasetBuilder} serves as an extension of {@link
 * AbstractQueryPlanDatasetBuilder} which gets applied only for COMPLETE OpenLineage events.
 * Filtering is done by verifying subclass of {@link SparkListenerEvent}
 *
 * @param <P>
 */
@Slf4j
public abstract class AbstractQueryPlanOutputDatasetBuilder<P extends LogicalPlan>
    extends AbstractQueryPlanDatasetBuilder<SparkListenerEvent, P, OpenLineage.OutputDataset>
    implements JobNameSuffixProvider<P> {

  public AbstractQueryPlanOutputDatasetBuilder(
      OpenLineageContext context, boolean searchDependencies) {
    super(context, searchDependencies);
  }

  @Override
  public boolean isDefinedAt(SparkListenerEvent event) {
    return true;
  }

  protected boolean includeDatasetVersion(SparkListenerEvent event) {
    return event instanceof SparkListenerSQLExecutionEnd;
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(P logicalPlan) {
    throw new UnsupportedOperationException("Unimplemented");
  }

  public List<OpenLineage.OutputDataset> delegate(LogicalPlan node, SparkListenerEvent event) {
    return delegate(
            context.getOutputDatasetQueryPlanVisitors(), context.getOutputDatasetBuilders(), event)
        .applyOrElse(
            node, ScalaConversionUtils.toScalaFn((lp) -> Collections.<OutputDataset>emptyList()))
        .stream()
        .collect(Collectors.toList());
  }

  @Override
  public Optional<String> jobNameSuffix(OpenLineageContext context) {
    return context
        .getQueryExecution()
        .map(qe -> qe.optimizedPlan())
        .map(p -> jobNameSuffixFromLogicalPlan(p))
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  public Optional<String> jobNameSuffixFromLogicalPlan(LogicalPlan p) {
    return Optional.of(p)
        .filter(plan -> this.isDefinedAtLogicalPlan(plan))
        .map(plan -> jobNameSuffix((P) plan))
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  protected String identToSuffix(Identifier identifier) {
    if (identifier.namespace() == null) {
      return StringUtils.EMPTY;
    }
    String suffix = String.join(SUFFIX_DELIMITER, identifier.namespace());
    if (StringUtils.isNotEmpty(suffix)) {
      suffix += SUFFIX_DELIMITER;
    }
    suffix += identifier.name();

    return suffix;
  }
}
