/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.agent.lifecycle.Rdds;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.ExternalRDD;
import org.apache.spark.sql.types.StructType;

/** Extracts input datasets from the file-like {@link RDD}s read by an {@link ExternalRDD}. */
public class ExternalRDDDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<ExternalRDD<?>> {

  public ExternalRDDDatasetBuilder(OpenLineageContext context) {
    super(context, true);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return x instanceof ExternalRDD;
  }

  @Override
  public List<InputDataset> apply(ExternalRDD<?> externalRDD) {
    List<RDD<?>> fileRdds = Rdds.findFileLikeRdds(externalRDD.rdd());
    StructType schema = externalRDD.schema();
    return PlanUtils.findDatasetIdentifiers(fileRdds).stream()
        .map(
            di ->
                schema != null
                    ? inputDataset().sparkDatasetBuilder().dataset(di).schema(schema).build()
                    : inputDataset().sparkDatasetBuilder().dataset(di).build())
        .collect(Collectors.toList());
  }
}
