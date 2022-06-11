/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunFacet;
import io.openlineage.spark.agent.util.PlanUtils;
import io.openlineage.spark.agent.util.SparkConfUtils;
import io.openlineage.spark.api.AbstractRunFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

@Slf4j
public class ParentRunFacetBuilder extends AbstractRunFacetBuilder<SparkListenerEvent> {
  private static final Map<Long, Entry<UUID, String>> executionIdToRunIdMap =
      new ConcurrentHashMap<>();

  public ParentRunFacetBuilder(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  protected void build(SparkListenerEvent event, BiConsumer<String, ? super RunFacet> consumer) {
    context
        .getSparkSession()
        .ifPresent(
            sess -> {
              Optional<ParentRunFacet> parentFacet = buildParentFacet(sess, event);
              parentFacet.ifPresent(facet -> consumer.accept("parent", facet));
            });
  }

  private Optional<ParentRunFacet> buildParentFacet(SparkSession sess, SparkListenerEvent event) {
    String parentRunId = null;
    String parentJobName = null;

    if (event instanceof SparkListenerSQLExecutionStart) {
      // If this is an ExecutionStart, we need to extract the runId and jobName
      // based on the executionId and the OpenLineageContext
      Long executionId = ((SparkListenerSQLExecutionStart) event).executionId();
      log.debug(
          "Setting execution Id of {} to run id of {}",
          String.valueOf(executionId),
          context.getRunUuid().toString());

      if (context.getQueryExecution().isPresent()) {
        SparkContext sparkContext = context.getSparkContext();
        SparkPlan sparkPlan = context.getQueryExecution().get().executedPlan();
        String jobName = PlanUtils.getSparkJobName(sparkContext, sparkPlan);
        log.debug("Execution Id of {} has a jobName of {}", String.valueOf(executionId), jobName);

        executionIdToRunIdMap.put(
            executionId, new AbstractMap.SimpleEntry<UUID, String>(context.getRunUuid(), jobName));
      } else {
        log.debug("Failed to set the execution context since queryExecution was missing");
      }

    } else if (event instanceof SparkListenerJobStart) {
      SparkListenerJobStart jobStart = (SparkListenerJobStart) event;
      String currentExecutionId = jobStart.properties().getProperty("spark.sql.execution.id");

      log.debug("Spark SQL Execution Id is: {}", currentExecutionId);

      // This property appears to be unique to Azure Databricks' implementation
      // of a connector to Azure Synapse SQL Pools. By extracting this and looking
      // up the OpenLineage RunId and JobName based on this execution Id, we can
      // identify the previous execution as the parent runId and parent job Name.
      if (jobStart.properties().containsKey("spark.sql.execution.parent")) {
        String sqlExecutionParentId =
            jobStart.properties().getProperty("spark.sql.execution.parent");

        log.debug(
            "Current ExecutionId {} has Parent Execution Id {}",
            currentExecutionId,
            sqlExecutionParentId);

        Optional<Entry<UUID, String>> parentEntry =
            executionParentToParentRun(sqlExecutionParentId);

        if (parentEntry.isPresent()) {
          // Based on the previous execution, set the parentRunId and parentJobName
          parentRunId = parentEntry.get().getKey().toString();
          parentJobName = parentEntry.get().getValue().toString();
        }
      }
    }

    // Get values for parent run and job name from configuration if they have
    // not been populated.
    if (parentRunId == null) {
      parentRunId =
          SparkConfUtils.findSparkConfigKey(
              sess.sparkContext().conf(), "openlineage.parentRunId", null);
    }
    if (parentJobName == null) {
      parentJobName =
          SparkConfUtils.findSparkConfigKey(
              sess.sparkContext().conf(), "openlineage.parentJobName", null);
    }

    String namespace =
        SparkConfUtils.findSparkConfigKey(
            sess.sparkContext().conf(), "openlineage.namespace", null);

    Optional<ParentRunFacet> parentRunFacet = Optional.empty();
    if (parentRunId != null && parentJobName != null && namespace != null) {
      parentRunFacet =
          Optional.of(
              context
                  .getOpenLineage()
                  .newParentRunFacetBuilder()
                  .run(
                      context
                          .getOpenLineage()
                          .newParentRunFacetRunBuilder()
                          .runId(UUID.fromString(parentRunId))
                          .build())
                  .job(
                      context
                          .getOpenLineage()
                          .newParentRunFacetJobBuilder()
                          .namespace(namespace)
                          .name(parentJobName)
                          .build())
                  .build());
    }

    if (event instanceof SparkListenerSQLExecutionEnd) {
      // If this is an ExecutionEnd, we can remove the information from the map
      Long executionId = ((SparkListenerSQLExecutionEnd) event).executionId();
      // Does the async event queue ever run out of sync?
      if (executionIdToRunIdMap.containsKey(executionId)) {
        log.info(String.format("Removing execution Id of %s", String.valueOf(executionId)));
        executionIdToRunIdMap.remove(executionId);
      }
    }

    return parentRunFacet.isPresent() ? parentRunFacet : Optional.empty();
  }

  private Optional<Entry<UUID, String>> executionParentToParentRun(String sqlExecutionParent) {
    // Test to see if the sqlExecutionParent was present, if so extract the stored
    // runid
    if (sqlExecutionParent != null) {
      if (executionIdToRunIdMap.containsKey(Long.parseLong(sqlExecutionParent))) {
        return Optional.of(executionIdToRunIdMap.get(Long.parseLong(sqlExecutionParent)));
      } else {
        return Optional.empty();
      }
    }
    return Optional.empty();
  }
}
