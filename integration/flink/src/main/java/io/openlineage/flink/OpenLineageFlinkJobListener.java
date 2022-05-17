package io.openlineage.flink;

import io.openlineage.flink.agent.ArgumentParser;
import io.openlineage.flink.agent.EventEmitter;
import io.openlineage.flink.agent.lifecycle.FlinkExecutionContext;
import io.openlineage.flink.agent.lifecycle.FlinkExecutionContextFactory;
import io.openlineage.flink.tracker.OpenLineageContinousJobTracker;
import io.openlineage.flink.tracker.OpenLineageContinousJobTrackerFactory;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Flink Listener registered within Flink Application which gets notified when application is
 * submitted and executed. After this, it uses {@link OpenLineageContinousJobTracker} to track
 * status and collect information (like checkpoints) from the running application.
 */
@Slf4j
public class OpenLineageFlinkJobListener implements JobListener {

  private final StreamExecutionEnvironment executionEnvironment;
  private final OpenLineageContinousJobTracker openLineageContinousJobTracker;
  private final Map<JobID, FlinkExecutionContext> jobContexts = new HashMap<>();

  public OpenLineageFlinkJobListener(StreamExecutionEnvironment executionEnvironment) {
    this.executionEnvironment = executionEnvironment;
    this.openLineageContinousJobTracker =
        OpenLineageContinousJobTrackerFactory.getTracker(executionEnvironment.getConfiguration());
    makeTransformationsArchivedList(executionEnvironment);
  }

  @Override
  public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    if (jobClient != null) {
      Field transformationsField =
          FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
      try {
        ArgumentParser args =
            ArgumentParser.parseConfiguration(
                executionEnvironment.getConfig().getGlobalJobParameters().toMap());
        List<Transformation<?>> transformations =
            ((ArchivedList<Transformation<?>>) transformationsField.get(executionEnvironment))
                .getValue();

        FlinkExecutionContext context =
            FlinkExecutionContextFactory.getContext(
                jobClient.getJobID(), new EventEmitter(args), transformations);

        jobContexts.put(jobClient.getJobID(), context);
        context.onJobSubmitted();
        log.info("Job submitted");
      } catch (URISyntaxException e) {
        log.error("Unable to parse open lineage endpoint. Lineage events will not be collected", e);
      } catch (IllegalAccessException e) {
        log.error("Can't access the field. ", e);
      }
    }
  }

  @Override
  public void onJobExecuted(
      @Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
    log.debug("JobExecutionResult: {}", jobExecutionResult);

    // start tracker when job execution starts
    log.info("OpenLineageContinousJobTracker is starting");
    openLineageContinousJobTracker.startTracking(jobContexts.get(jobExecutionResult.getJobID()));
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
