/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.lifecycle;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.spark.agent.EventEmitter;
import io.openlineage.spark.agent.JobMetricsHolder;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageRunStatus;
import io.openlineage.spark.api.SparkOpenLineageConfig;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.JobFailed;
import org.apache.spark.scheduler.JobSucceeded$;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

/**
 * Verifies that {@link RddExecutionContext} applies {@code
 * spark.openlineage.dataset.removePath.pattern} to inputs/outputs of RDD jobs, same as {@link
 * OpenLineageRunEventBuilder} already does for non-RDD jobs. See
 * https://github.com/OpenLineage/OpenLineage/issues/4719
 *
 * <p>Also verifies that an RDD run which emitted its START event is always closed with a terminal
 * event, even when no output dataset could be detected for the job.
 */
class RddExecutionContextTest {

  private static final String REMOVE_PATH_PATTERN = "spark.openlineage.dataset.removePath.pattern";

  private final OpenLineageContext olContext = mock(OpenLineageContext.class);
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
  private final EventEmitter eventEmitter = mock(EventEmitter.class);
  private final OpenLineageRunEventBuilder runEventBuilder =
      new OpenLineageRunEventBuilder(olContext, mock(OpenLineageEventHandlerFactory.class));
  private final RddExecutionContext context =
      new RddExecutionContext(olContext, eventEmitter, runEventBuilder);

  private SparkConf sparkConf;

  @AfterEach
  void cleanupMetrics() {
    JobMetricsHolder.getInstance().cleanUpAll();
  }

  @BeforeEach
  void setup() {
    sparkConf = mock(SparkConf.class);
    SparkContext sparkContext = mock(SparkContext.class);
    when(sparkContext.conf()).thenReturn(sparkConf);
    when(sparkContext.getConf()).thenReturn(sparkConf);
    when(olContext.getSparkContext()).thenReturn(Optional.of(sparkContext));
    when(olContext.getOpenLineage()).thenReturn(openLineage);
    when(olContext.getOpenLineageConfig()).thenReturn(new SparkOpenLineageConfig());
    when(olContext.getLineageRunStatus()).thenReturn(new OpenLineageRunStatus());
  }

  @Test
  void buildInputsRemovesConfiguredPathPatternForRddJobs() {
    when(sparkConf.contains(REMOVE_PATH_PATTERN)).thenReturn(true);
    when(sparkConf.get(REMOVE_PATH_PATTERN)).thenReturn("(?<remove>tmp)");

    List<DatasetIdentifier> inputs =
        Collections.singletonList(new DatasetIdentifier("/tmp/input.csv", "file"));

    List<InputDataset> result = context.buildInputs(inputs, false);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getName()).isEqualTo("//input.csv");
  }

  @Test
  void buildOutputsRemovesConfiguredPathPatternForRddJobs() {
    when(sparkConf.contains(REMOVE_PATH_PATTERN)).thenReturn(true);
    when(sparkConf.get(REMOVE_PATH_PATTERN)).thenReturn("(?<remove>tmp)");

    List<URI> outputs = Collections.singletonList(URI.create("file:///tmp/output.csv"));

    List<OutputDataset> result = context.buildOutputs(outputs, false);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getName()).isEqualTo("//output.csv");
  }

  @Test
  void buildInputsLeavesNameUnchangedWhenPatternNotConfigured() {
    when(sparkConf.contains(REMOVE_PATH_PATTERN)).thenReturn(false);

    List<DatasetIdentifier> inputs =
        Collections.singletonList(new DatasetIdentifier("/tmp/input.csv", "file"));

    List<InputDataset> result = context.buildInputs(inputs, false);

    assertThat(result).hasSize(1);
    assertThat(result.get(0).getName()).isEqualTo("/tmp/input.csv");
  }

  @Test
  void endWithoutStartCleansMetricsAndEmitsNoEvent() {
    SparkListenerJobEnd jobEnd = mock(SparkListenerJobEnd.class);
    when(jobEnd.jobId()).thenReturn(51);
    when(jobEnd.jobResult()).thenReturn(JobSucceeded$.MODULE$);
    JobMetricsHolder holder = JobMetricsHolder.getInstance();
    holder.addJobStages(51, Collections.singleton(510));

    context.end(jobEnd);

    verify(eventEmitter, never()).emit(any());
    assertThat(holder.getJobStagesSize()).isZero();
    assertThat(holder.getStageMetricsSize()).isZero();
    assertThat(holder.getJobMetricsSize()).isZero();
  }

  @Test
  void successfulOutputlessJobEmitsStartAndComplete() {
    context.start(jobStart(1));
    context.end(successfulJobEnd(1));

    List<RunEvent> events = emittedEvents(2);
    RunEvent start = events.get(0);
    RunEvent complete = events.get(1);

    assertThat(start.getEventType()).isEqualTo(EventType.START);
    assertThat(complete.getEventType()).isEqualTo(EventType.COMPLETE);
    assertThat(complete.getRun().getRunId()).isEqualTo(start.getRun().getRunId());
    assertThat(complete.getJob().getName()).isEqualTo(start.getJob().getName());
    assertThat(complete.getJob().getNamespace()).isEqualTo(start.getJob().getNamespace());
    assertThat(complete.getInputs()).isEmpty();
    assertThat(complete.getOutputs()).isEmpty();
  }

  @Test
  void successfulOutputlessJobWithInputsEmitsCompleteWithInputs() throws Exception {
    setDetectedInputs(Collections.singletonList(new DatasetIdentifier("/tmp/input.csv", "file")));

    context.start(jobStart(1));
    context.end(successfulJobEnd(1));

    List<RunEvent> events = emittedEvents(2);
    RunEvent start = events.get(0);
    RunEvent complete = events.get(1);

    assertThat(start.getEventType()).isEqualTo(EventType.START);
    assertThat(complete.getEventType()).isEqualTo(EventType.COMPLETE);
    assertThat(complete.getRun().getRunId()).isEqualTo(start.getRun().getRunId());
    assertThat(complete.getInputs())
        .extracting(InputDataset::getName)
        .containsExactly("/tmp/input.csv");
    assertThat(complete.getOutputs()).isEmpty();
  }

  @Test
  void failedOutputlessJobEmitsStartAndFail() {
    context.start(jobStart(1));
    context.end(new SparkListenerJobEnd(1, 2L, new JobFailed(new RuntimeException("job failed"))));

    List<RunEvent> events = emittedEvents(2);
    RunEvent start = events.get(0);
    RunEvent fail = events.get(1);

    assertThat(start.getEventType()).isEqualTo(EventType.START);
    assertThat(fail.getEventType()).isEqualTo(EventType.FAIL);
    assertThat(fail.getRun().getRunId()).isEqualTo(start.getRun().getRunId());
    assertThat(fail.getJob().getName()).isEqualTo(start.getJob().getName());
    assertThat(fail.getRun().getFacets().getAdditionalProperties()).containsKey("spark.exception");
    assertThat(fail.getOutputs()).isEmpty();
  }

  @Test
  void rddEventsDisabledEmitsNoEvents() {
    SparkOpenLineageConfig.FilterConfig filterConfig =
        mock(SparkOpenLineageConfig.FilterConfig.class);
    when(filterConfig.getRddEventsDisabled()).thenReturn(true);
    SparkOpenLineageConfig config = mock(SparkOpenLineageConfig.class);
    when(config.getFilterConfig()).thenReturn(filterConfig);
    when(olContext.getOpenLineageConfig()).thenReturn(config);

    context.start(jobStart(1));
    context.end(successfulJobEnd(1));

    verify(eventEmitter, never()).emit(any());
  }

  @Test
  void jobWithDetectedOutputEmitsStartAndCompleteWithOutput() throws Exception {
    setDetectedOutputs(Collections.singletonList(URI.create("file:///tmp/output.csv")));

    context.start(jobStart(1));
    context.end(successfulJobEnd(1));

    List<RunEvent> events = emittedEvents(2);
    RunEvent start = events.get(0);
    RunEvent complete = events.get(1);

    assertThat(start.getEventType()).isEqualTo(EventType.START);
    assertThat(complete.getEventType()).isEqualTo(EventType.COMPLETE);
    assertThat(complete.getRun().getRunId()).isEqualTo(start.getRun().getRunId());
    assertThat(start.getOutputs()).hasSize(1);
    assertThat(complete.getOutputs()).hasSize(1);
    assertThat(complete.getOutputs().get(0).getName()).isEqualTo("/tmp/output.csv");
  }

  @Test
  void completeEventPreservesRunFacets() {
    context.start(jobStart(1));
    context.end(successfulJobEnd(1));

    RunEvent complete = emittedEvents(2).get(1);

    assertThat(complete.getRun().getFacets().getParent()).isNotNull();
    assertThat(complete.getRun().getFacets().getProcessing_engine()).isNotNull();
    assertThat(complete.getRun().getFacets().getAdditionalProperties())
        .containsKey("spark_properties");
  }

  private List<RunEvent> emittedEvents(int expectedCount) {
    ArgumentCaptor<RunEvent> lineageEvent = ArgumentCaptor.forClass(RunEvent.class);
    verify(eventEmitter, times(expectedCount)).emit(lineageEvent.capture());
    return lineageEvent.getAllValues();
  }

  private SparkListenerJobStart jobStart(int jobId) {
    return new SparkListenerJobStart(
        jobId, 1L, ScalaConversionUtils.asScalaSeqEmpty(), new Properties());
  }

  private SparkListenerJobEnd successfulJobEnd(int jobId) {
    return new SparkListenerJobEnd(jobId, 2L, JobSucceeded$.MODULE$);
  }

  private void setDetectedInputs(List<DatasetIdentifier> inputs) throws Exception {
    setContextField("inputs", inputs);
  }

  private void setDetectedOutputs(List<URI> outputs) throws Exception {
    setContextField("outputs", outputs);
  }

  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  private void setContextField(String fieldName, Object value) throws Exception {
    Field field = RddExecutionContext.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(context, value);
  }
}
