/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.JobBuilder;
import io.openlineage.client.OpenLineage.ParentRunFacet;
import io.openlineage.client.OpenLineage.RunEventBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import org.apache.spark.rdd.RDD;
import org.apache.spark.scheduler.ActiveJob;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.Stage;
import org.apache.spark.scheduler.StageInfo;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;

/**
 * Class to contain properties for building a single OpenLineage event. Should be passed to {@link
 * OpenLineageRunEventBuilder}.
 */
@Getter
@Builder
public class OpenLineageRunEventContext {

  private ParentRunFacet applicationParentRunFacet;
  private RunEventBuilder runEventBuilder;
  private JobBuilder jobBuilder;
  private OpenLineage.JobFacetsBuilder jobFacetsBuilder;
  private OpenLineage.RunFacetsBuilder runFacetsBuilder;
  private Optional<StageInfo> stageInfo;
  private Optional<Integer> jobId;
  private Optional<UUID> overwriteRunId;
  private SparkListenerEvent event;

  public List<Object> loadNodes(Map<Integer, Stage> stageMap, Map<Integer, ActiveJob> jobMap) {
    List<Object> nodes = new ArrayList<>();
    nodes.add(event);

    if (stageInfo.isPresent()) {
      Stage stage = stageMap.get(stageInfo.get().stageId());
      RDD<?> rdd = stage.rdd();

      nodes.addAll(Arrays.asList(stageInfo, stage));
      nodes.addAll(Rdds.flattenRDDs(rdd));
    } else if (jobId.isPresent() && jobMap.containsKey(jobId.get())) {
      ActiveJob activeJob = jobMap.get(jobId.get());
      nodes.add(activeJob);
      nodes.addAll(Rdds.flattenRDDs(activeJob.finalStage().rdd()));
    }

    return nodes;
  }

  @SuppressWarnings("PMD.UnusedPrivateField")
  public static class OpenLineageRunEventContextBuilder {
    private Optional<StageInfo> stageInfo = Optional.empty();
    private Optional<Integer> jobId = Optional.empty();
    private Optional<UUID> overwriteRunId = Optional.empty();

    OpenLineageRunEventContextBuilder event(SparkListenerStageSubmitted event) {
      this.stageInfo = Optional.of(event.stageInfo());
      this.event = event;
      return this;
    }

    OpenLineageRunEventContextBuilder event(SparkListenerStageCompleted event) {
      this.stageInfo = Optional.of(event.stageInfo());
      this.event = event;
      return this;
    }

    OpenLineageRunEventContextBuilder event(SparkListenerSQLExecutionStart event) {
      this.event = event;
      return this;
    }

    OpenLineageRunEventContextBuilder event(SparkListenerSQLExecutionEnd event) {
      this.event = event;
      return this;
    }

    OpenLineageRunEventContextBuilder event(SparkListenerJobStart event) {
      this.event = event;
      this.jobId = Optional.of(event.jobId());
      return this;
    }

    OpenLineageRunEventContextBuilder event(SparkListenerJobEnd event) {
      this.event = event;
      this.jobId = Optional.of(event.jobId());
      return this;
    }

    OpenLineageRunEventContextBuilder event(SparkListenerApplicationStart event) {
      this.event = event;
      return this;
    }

    OpenLineageRunEventContextBuilder event(SparkListenerApplicationEnd event) {
      this.event = event;
      return this;
    }
  }
}
