/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.agent.util.FacetUtils;
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
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
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
    // Computing the dataset version can require an extra catalog lookup (for example, Iceberg's
    // IcebergHandler#getDatasetVersion calls TableCatalog#loadTable()). When the datasetVersion
    // facet is disabled, skip that lookup entirely instead of computing it and discarding the
    // result at emit time - the extra load is not just wasted work, it can also race with a
    // concurrent, not-yet-committed write and repopulate a catalog-side table cache with a stale
    // pre-commit reference (see https://github.com/apache/iceberg/issues/10493 and
    // https://github.com/OpenLineage/OpenLineage/issues/4040).
    return event instanceof SparkListenerSQLExecutionEnd
        && !FacetUtils.isFacetDisabled(context, "datasetVersion");
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

  protected List<OpenLineage.OutputDataset> getTableOutputs(DataSourceV2Relation plan) {
    Table table = plan.table();

    return context
        .getSparkExtensionVisitorWrapper()
        .getOutputs(table, table.getClass().getName())
        .getLeft();
  }
}
