/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.spark.api.AbstractQueryPlanInputDatasetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.utils.CopyIntoCommandUtils;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

/**
 * Extracts input datasets from the Databricks-specific {@code CopyIntoCommand} or {@code
 * CopyIntoCommandEdge}. The source is typically a file path (e.g., S3, ADLS, DBFS, Unity Catalog
 * Volumes). Since these classes belong to the Databricks runtime, reflection is used to access
 * source-related methods.
 */
@Slf4j
public class CopyIntoCommandInputDatasetBuilder
    extends AbstractQueryPlanInputDatasetBuilder<LogicalPlan> {

  public CopyIntoCommandInputDatasetBuilder(OpenLineageContext context) {
    super(context, false);
  }

  @Override
  public boolean isDefinedAtLogicalPlan(LogicalPlan x) {
    return CopyIntoCommandUtils.isCopyIntoCommand(x);
  }

  @Override
  protected List<InputDataset> apply(SparkListenerEvent event, LogicalPlan x) {
    String sourcePath = extractSourcePath(x);
    if (sourcePath != null && !sourcePath.isEmpty()) {
      try {
        return Collections.singletonList(
            inputDataset().sparkDatasetBuilder().dataset(URI.create(sourcePath)).build());
      } catch (Exception e) {
        log.warn("Failed to construct input dataset from COPY INTO source path: {}", sourcePath, e);
      }
    }

    LogicalPlan query = extractQuery(x);
    if (query != null) {
      return delegate(query, event);
    }

    return Collections.emptyList();
  }

  @SuppressWarnings("unchecked")
  private String extractSourcePath(LogicalPlan x) {
    try {
      Object options = MethodUtils.invokeExactMethod(x, "sourceOptions", new Object[] {});
      if (options instanceof Map) {
        Object path = ((Map<String, String>) options).get("path");
        if (path instanceof String) {
          return (String) path;
        }
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.debug("Cannot extract sourceOptions from Databricks CopyIntoCommand", e);
    }

    try {
      Object result = MethodUtils.invokeExactMethod(x, "sourcePath", new Object[] {});
      if (result instanceof String) {
        return (String) result;
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.debug("Cannot extract sourcePath from Databricks CopyIntoCommand", e);
    }

    return null;
  }

  private LogicalPlan extractQuery(LogicalPlan x) {
    try {
      Object result = MethodUtils.invokeExactMethod(x, "query", new Object[] {});
      if (result instanceof LogicalPlan) {
        return (LogicalPlan) result;
      }
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.debug("Cannot extract query from Databricks CopyIntoCommand", e);
    }
    return null;
  }
}
