/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import io.openlineage.spark.agent.JobMetricsHolder.Metric;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Owns compact job metrics while SQL executions in the same root execution are still active. */
class JobMetricsLifecycleManager {
  private final JobMetricsHolder jobMetrics;
  private final Map<Long, Long> executionRoots = new HashMap<>();
  private final Map<Long, Set<Long>> activeExecutionsByRoot = new HashMap<>();
  private final Map<Integer, Long> jobRoots = new HashMap<>();
  private final Map<Long, Set<Integer>> jobsByRoot = new HashMap<>();
  private final Set<Integer> completedJobs = new HashSet<>();

  JobMetricsLifecycleManager(JobMetricsHolder jobMetrics) {
    this.jobMetrics = jobMetrics;
  }

  synchronized void registerExecution(long executionId, long rootExecutionId) {
    long rootId = executionRoots.getOrDefault(executionId, rootExecutionId);
    executionRoots.put(executionId, rootId);
    activeExecutionsByRoot.computeIfAbsent(rootId, ignored -> new HashSet<>()).add(executionId);
  }

  synchronized void registerJob(int jobId, long executionId, Optional<Long> rootExecutionId) {
    long rootId = executionRoots.getOrDefault(executionId, rootExecutionId.orElse(executionId));
    registerExecution(executionId, rootId);

    Long previousRootId = jobRoots.put(jobId, rootId);
    if (previousRootId != null && previousRootId != rootId) {
      removeJobFromRoot(jobId, previousRootId);
    }
    jobsByRoot.computeIfAbsent(rootId, ignored -> new HashSet<>()).add(jobId);
  }

  synchronized void completeJob(int jobId) {
    Map<Metric, Number> metrics = jobMetrics.completeJob(jobId);
    Long rootId = jobRoots.get(jobId);
    if (metrics.isEmpty()
        || rootId == null
        || activeExecutionsByRoot
            .getOrDefault(rootId, java.util.Collections.emptySet())
            .isEmpty()) {
      cleanUpJob(jobId);
    } else {
      completedJobs.add(jobId);
    }
  }

  synchronized void endExecution(long executionId) {
    Long rootId = executionRoots.remove(executionId);
    if (rootId == null) {
      return;
    }

    Set<Long> activeExecutions = activeExecutionsByRoot.get(rootId);
    if (activeExecutions == null) {
      return;
    }
    activeExecutions.remove(executionId);
    if (!activeExecutions.isEmpty()) {
      return;
    }

    activeExecutionsByRoot.remove(rootId);
    Set<Integer> jobs = jobsByRoot.remove(rootId);
    if (jobs != null) {
      new HashSet<>(jobs)
          .forEach(
              jobId -> {
                if (completedJobs.contains(jobId)) {
                  cleanUpJob(jobId);
                } else {
                  jobRoots.remove(jobId);
                }
              });
    }
  }

  synchronized void cleanUpJob(int jobId) {
    jobMetrics.cleanUp(jobId);
    completedJobs.remove(jobId);
    Long rootId = jobRoots.remove(jobId);
    if (rootId != null) {
      removeJobFromRoot(jobId, rootId);
    }
  }

  synchronized void cleanUpAll() {
    executionRoots.clear();
    activeExecutionsByRoot.clear();
    jobRoots.clear();
    jobsByRoot.clear();
    completedJobs.clear();
    jobMetrics.cleanUpAll();
  }

  synchronized int getExecutionGroupCount() {
    return activeExecutionsByRoot.size();
  }

  synchronized int getPendingJobCount() {
    return jobRoots.size();
  }

  private void removeJobFromRoot(int jobId, long rootId) {
    Set<Integer> jobs = jobsByRoot.get(rootId);
    if (jobs != null) {
      jobs.remove(jobId);
      if (jobs.isEmpty()) {
        jobsByRoot.remove(rootId);
      }
    }
  }
}
