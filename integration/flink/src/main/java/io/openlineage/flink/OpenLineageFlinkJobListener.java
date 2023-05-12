/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink;

import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.tracker.OpenLineageContinousJobTrackerFactory;
import io.openlineage.flink.visitor.lifecycle.FlinkExecutionContext;
import io.openlineage.flink.visitor.lifecycle.FlinkExecutionContextFactory;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;

/**
 * Flink Listener registered within Flink Application which gets notified when application is
 * submitted and executed. After this, it uses {@link OpenLineageContinousJobTracker} to track
 * status and collect information (like checkpoints) from the running application.
 */
@Slf4j
@Builder
public class OpenLineageFlinkJobListener implements JobListener {

  public static final String DEFAULT_JOB_NAMESPACE = "flink_jobs";
  private final StreamExecutionEnvironment executionEnvironment;
  private final OpenLineageContinousJobTracker jobTracker;
  private final String jobNamespace;
  private final String jobName;
  private final Map<JobID, FlinkExecutionContext> jobContexts = new HashMap<>();

  public static OpenLineageFlinkJobListenerBuilder builder() {
    return new OpenLineageFlinkJobListenerInternalBuilder();
  }

  static class OpenLineageFlinkJobListenerInternalBuilder
      extends OpenLineageFlinkJobListenerBuilder {
    @Override
    public OpenLineageFlinkJobListener build() {
      Validate.notNull(super.executionEnvironment, "StreamExecutionEnvironment has to be provided");

      if (super.jobNamespace == null) {
        super.jobNamespace(DEFAULT_JOB_NAMESPACE);
      }

      if (super.jobName == null) {
        super.jobName(StreamGraphGenerator.DEFAULT_STREAMING_JOB_NAME);
      }

      return super.build();
    }

    @Override
    public OpenLineageFlinkJobListenerBuilder executionEnvironment(
        StreamExecutionEnvironment executionEnvironment) {
      super.executionEnvironment(executionEnvironment);
      super.jobTracker(
          OpenLineageContinousJobTrackerFactory.getTracker(
              executionEnvironment.getConfiguration()));
      makeTransformationsArchivedList(executionEnvironment);
      return this;
    }

    private void makeTransformationsArchivedList(StreamExecutionEnvironment executionEnvironment) {
      try {
        Field transformations =
            FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
        ArrayList<Transformation<?>> previousTransformationList =
            (ArrayList<Transformation<?>>)
                FieldUtils.readField(transformations, executionEnvironment, true);
        List<Transformation<?>> transformationList =
            new ArchivedList<>(
                Optional.ofNullable(previousTransformationList).orElse(new ArrayList<>()));
        FieldUtils.writeField(transformations, executionEnvironment, transformationList, true);
      } catch (IllegalAccessException e) {
        log.error("Failed to rewrite transformations");
      }
    }
  }

  @Override
  public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    if (jobClient == null) {
      return;
    }

    try {
      start(jobClient);
    } catch (Exception | NoClassDefFoundError | NoSuchFieldError e) {
      log.error("Failed to notify OpenLineage about start", e);
    }
  }

  void start(JobClient jobClient) {
    Field transformationsField =
        FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
    try {
      List<Transformation<?>> transformations =
          ((ArchivedList<Transformation<?>>) transformationsField.get(executionEnvironment))
              .getValue();

      FlinkExecutionContext context =
          FlinkExecutionContextFactory.getContext(
              jobNamespace, jobName, jobClient.getJobID(), transformations);

      jobContexts.put(jobClient.getJobID(), context);
      context.onJobSubmitted();
      log.info("Job submitted");

      log.info("OpenLineageContinousJobTracker is starting");
      jobTracker.startTracking(context);
    } catch (IllegalAccessException e) {
      log.error("Can't access the field. ", e);
    }
  }

  @Override
  public void onJobExecuted(
      @Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
    try {
      finish(jobExecutionResult, throwable);
    } catch (Exception | NoClassDefFoundError | NoSuchFieldError e) {
      log.error("Failed to notify OpenLineage about complete", e);
    }
  }

  void finish(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
    if (jobExecutionResult instanceof DetachedJobExecutionResult) {
      jobContexts.remove(jobExecutionResult.getJobID());
      log.warn(
          "Job running in detached mode. Set execution.attached to true if you want to emit completed events.");
      return;
    }

    if (jobExecutionResult != null) {
      jobContexts.remove(jobExecutionResult.getJobID()).onJobCompleted(jobExecutionResult);
    } else {
      // We don't have jobId when failed, so we need to assume that only existing context is that
      // job
      if (jobContexts.size() == 1) { // NOPMD
        Map.Entry<JobID, FlinkExecutionContext> entry =
            jobContexts.entrySet().stream().findFirst().get();
        jobContexts.remove(entry.getKey()).onJobFailed(throwable);
      }
    }
  }

  static class ArchivedList<T> extends ArrayList<T> {
    @Getter List<T> value;

    public ArchivedList(Collection<T> collection) {
      super(collection);
      value = new ArrayList<>(collection);
    }

    @Override
    public void clear() {
      value = new ArrayList<>(this);
      super.clear();
    }
  }
}
