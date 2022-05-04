package io.openlineage.flink;

import io.openlineage.flink.agent.ArgumentParser;
import io.openlineage.flink.agent.EventEmitter;
import io.openlineage.flink.agent.lifecycle.ExecutionContext;
import io.openlineage.flink.agent.lifecycle.FlinkExecutionContext;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class OpenLineageFlinkJobListener implements JobListener {

  private final StreamExecutionEnvironment executionEnvironment;

  public OpenLineageFlinkJobListener(StreamExecutionEnvironment executionEnvironment) {
    makeTransformationsArchivedList(executionEnvironment);
    this.executionEnvironment = executionEnvironment;
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
                .getArchive();
        ExecutionContext context =
            new FlinkExecutionContext(jobClient.getJobID(), new EventEmitter(args));

        context.onJobSubmitted(jobClient, transformations);
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
  }

  private void makeTransformationsArchivedList(StreamExecutionEnvironment executionEnvironment) {
    try {
      Field transformations =
          FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
      ArrayList<Transformation<?>> previousTransformationList =
          (ArrayList<Transformation<?>>)
              FieldUtils.readField(transformations, executionEnvironment, true);
      List<Transformation<?>> transformationList = new ArchivedList<>(previousTransformationList);
      FieldUtils.writeField(transformations, executionEnvironment, transformationList, true);
    } catch (IllegalAccessException e) {
      log.error("Failed to rewrite transformations");
    }
  }

  static class ArchivedList<T> extends ArrayList<T> {
    @Getter List<T> archive;

    public ArchivedList(Collection<T> collection) {
      super(collection);
    }

    @Override
    public void clear() {
      archive = new ArrayList<>(this);
      super.clear();
    }
  }
}
