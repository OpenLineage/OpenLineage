/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.job.JobConfig;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.api.OpenLineageContext.JobIdentifier;
import io.openlineage.flink.api.OpenLineageContextFactory;
import io.openlineage.flink.client.Versions;
import io.openlineage.flink.config.FlinkConfigParser;
import io.openlineage.flink.config.FlinkOpenLineageConfig;
import io.openlineage.flink.facets.FlinkJobDetailsFacet;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Emits one OpenLineage run through the official Flink client and transport. */
final class SqlRunLifecycle {
  private static final Logger LOG = LoggerFactory.getLogger(SqlRunLifecycle.class);
  private final OpenLineageContext context;
  private final String jobName;
  private final String jobNamespace;
  private final String processingType;
  private final String statement;
  private final List<OpenLineage.InputDataset> inputs;
  private final List<OpenLineage.OutputDataset> outputs;
  private final AtomicBoolean started = new AtomicBoolean();
  private final AtomicBoolean terminal = new AtomicBoolean();
  private final AtomicBoolean closed = new AtomicBoolean();

  private SqlRunLifecycle(
      OpenLineageContext context,
      String jobName,
      String jobNamespace,
      String processingType,
      String statement,
      List<OpenLineage.InputDataset> inputs,
      List<OpenLineage.OutputDataset> outputs) {
    this.context = context;
    this.jobName = jobName;
    this.jobNamespace = jobNamespace;
    this.processingType = processingType;
    this.statement = statement;
    this.inputs = inputs;
    this.outputs = outputs;
  }

  static SqlRunLifecycle create(SqlCapture capture, String flinkJobName, String processingType) {
    FlinkOpenLineageConfig config = FlinkConfigParser.parse(capture.configuration);
    JobConfig job = config.getJobConfig();
    String namespace =
        job != null && job.getNamespace() != null ? job.getNamespace() : "flink-jobs";
    String name = job != null && job.getName() != null ? job.getName() : flinkJobName;
    OpenLineageContext context =
        OpenLineageContextFactory.fromConfig(config).processingType(processingType).build();
    try {
      SqlOpenLineageConverter converter = new SqlOpenLineageConverter(context);
      List<OpenLineage.InputDataset> inputs = converter.inputs(capture.datasets);
      List<OpenLineage.OutputDataset> outputs = converter.outputs(capture.datasets);
      if (outputs.isEmpty())
        throw new IllegalArgumentException("SQL capture has no output dataset");
      return new SqlRunLifecycle(
          context, name, namespace, processingType, capture.statement, inputs, outputs);
    } catch (RuntimeException error) {
      context.getEventEmitter().close();
      throw error;
    }
  }

  void start(JobClient client, Runnable afterTerminal) {
    if (!started.compareAndSet(false, true)) return;
    try {
      context.setJobId(
          JobIdentifier.builder()
              .jobNamespace(jobNamespace)
              .jobName(jobName)
              .flinkJobId(client.getJobID())
              .build());
    } catch (RuntimeException error) {
      LOG.warn("Could not initialize the Flink SQL run", error);
      terminal.set(true);
      finish(afterTerminal);
      return;
    }
    try {
      emit(EventType.START, null);
    } catch (RuntimeException error) {
      LOG.warn("Could not emit the Flink START event", error);
    }
    try {
      client
          .getJobExecutionResult()
          .whenComplete(
              (result, failure) -> {
                try {
                  if (terminal.compareAndSet(false, true)) {
                    emit(failure == null ? EventType.COMPLETE : EventType.FAIL, failure);
                  }
                } finally {
                  finish(afterTerminal);
                }
              });
    } catch (RuntimeException error) {
      try {
        if (terminal.compareAndSet(false, true)) emit(EventType.FAIL, error);
      } catch (RuntimeException emitError) {
        LOG.warn("Could not emit the Flink FAIL event", emitError);
      } finally {
        finish(afterTerminal);
      }
    }
  }

  private void finish(Runnable afterTerminal) {
    try {
      close();
    } finally {
      afterTerminal.run();
    }
  }

  private void emit(EventType type, Throwable error) {
    OpenLineage ol = context.getOpenLineage();
    OpenLineage.JobFacetsBuilder jobFacets =
        ol.newJobFacetsBuilder()
            .jobType(
                ol.newJobTypeJobFacetBuilder()
                    .jobType("JOB")
                    .processingType(processingType)
                    .integration("FLINK")
                    .build());
    if (statement != null && !statement.isBlank()) {
      jobFacets.sql(ol.newSQLJobFacetBuilder().query(statement).dialect("flink").build());
    }
    OpenLineage.RunFacetsBuilder runFacets =
        ol.newRunFacetsBuilder()
            .processing_engine(
                ol.newProcessingEngineRunFacetBuilder()
                    .name("flink")
                    .version(EnvironmentInformation.getVersion())
                    .openlineageAdapterVersion(Versions.getVersion())
                    .build())
            .put(
                "flink_job",
                new FlinkJobDetailsFacet(context.getJobId().getFlinkJobId().toString()));
    if (error != null) {
      StringWriter stack = new StringWriter();
      error.printStackTrace(new PrintWriter(stack));
      runFacets.errorMessage(
          ol.newErrorMessageRunFacet(error.toString(), "JAVA", stack.toString()));
    }
    OpenLineage.RunEvent event =
        ol.newRunEventBuilder()
            .eventTime(ZonedDateTime.now())
            .eventType(type)
            .run(ol.newRunBuilder().runId(context.getRunUuid()).facets(runFacets.build()).build())
            .job(
                ol.newJobBuilder()
                    .namespace(jobNamespace)
                    .name(jobName)
                    .facets(jobFacets.build())
                    .build())
            .inputs(inputs)
            .outputs(outputs)
            .build();
    context
        .getCircuitBreaker()
        .run(
            () -> {
              context.getEventEmitter().emit(event);
              return null;
            });
    LOG.info("{} runId={} job={} emitted", type, context.getRunUuid(), jobName);
    System.out.println(
        "[openlineage-flink-sql] "
            + type
            + " runId="
            + context.getRunUuid()
            + " job="
            + jobName
            + " emitted");
  }

  void close() {
    if (closed.compareAndSet(false, true)) context.getEventEmitter().close();
  }
}
