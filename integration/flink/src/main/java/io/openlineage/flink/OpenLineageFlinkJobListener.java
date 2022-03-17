package io.openlineage.flink;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Visitor;
import io.openlineage.flink.visitor.VisitorFactory;
import io.openlineage.flink.visitor.VisitorFactoryImpl;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
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
  private final OpenLineageContext openLineageContext;

  public OpenLineageFlinkJobListener(StreamExecutionEnvironment executionEnvironment) {
    makeTransformationsArchivedList(executionEnvironment);
    this.executionEnvironment = executionEnvironment;
    this.openLineageContext =
        OpenLineageContext.builder()
            .streamExecutionEnvironment(Optional.of(executionEnvironment))
            .openLineage(new OpenLineage(URI.create("")))
            .build();
  }

  @Override
  public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
    if (jobClient != null) {
      log.debug("JobId: {}", jobClient.getJobID());
      Field transformationsField =
          FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
      try {
        List<Transformation<?>> transformations =
          ((ArchivedList<Transformation<?>>) transformationsField.get(executionEnvironment)).getArchive();
        TransformationUtils converter = new TransformationUtils();

        List<SinkLineage> sinkLineages = converter.convertToVisitable(transformations);
        VisitorFactory visitorFactory = new VisitorFactoryImpl();

        for (var lineage : sinkLineages) {
          List<OpenLineage.InputDataset> inputDatasets =
              getInputDatasets(visitorFactory, lineage.getSources());
          List<OpenLineage.OutputDataset> outputDatasets =
              getOutputDatasets(visitorFactory, lineage.getSink());
          createRunEventAndEmit(inputDatasets, outputDatasets);
        }

      } catch (IllegalAccessException e) {
        log.error("Can't access the field. ", e);
      }
    }
  }

  private void createRunEventAndEmit(
      List<OpenLineage.InputDataset> inputDatasets,
      List<OpenLineage.OutputDataset> outputDatasets) {
    OpenLineage.RunEvent runEvent =
        openLineageContext
            .getOpenLineage()
            .newRunEventBuilder()
            .inputs(inputDatasets)
            .outputs(outputDatasets)
            .build();

    log.debug("RunEvent: {}", runEvent);
  }

  private List<OpenLineage.InputDataset> getInputDatasets(
      VisitorFactory visitorFactory, List<Object> sources) {
    List<OpenLineage.InputDataset> inputDatasets = new ArrayList<>();
    List<Visitor<OpenLineage.InputDataset>> inputVisitors =
        visitorFactory.getInputVisitors(openLineageContext);

    for (var transformation : sources) {
      inputDatasets.addAll(
          inputVisitors.stream()
              .filter(inputVisitor -> inputVisitor.isDefinedAt(transformation))
              .map(inputVisitor -> inputVisitor.apply(transformation))
              .flatMap(List::stream)
              .collect(Collectors.toList()));
    }
    return inputDatasets;
  }

  private List<OpenLineage.OutputDataset> getOutputDatasets(
      VisitorFactory visitorFactory, Object sink) {
    List<OpenLineage.OutputDataset> outputDatasets = new ArrayList<>();
    List<Visitor<OpenLineage.OutputDataset>> outputVisitors =
        visitorFactory.getOutputVisitors(openLineageContext);

    outputDatasets.addAll(
        outputVisitors.stream()
            .filter(inputVisitor -> inputVisitor.isDefinedAt(sink))
            .map(inputVisitor -> inputVisitor.apply(sink))
            .flatMap(List::stream)
            .collect(Collectors.toList()));
    return outputDatasets;
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
      List<Transformation<?>> transformationList =
          new ArchivedList<>(previousTransformationList);
      FieldUtils.writeField(transformations, executionEnvironment, transformationList, true);
    } catch (IllegalAccessException e) {
      log.error("Failed to rewrite transformations");
    }
  }

  static class ArchivedList<T> extends ArrayList<T> {
    @Getter
    List<T> archive;

    public ArchivedList(Collection<T> collection) {
      super(collection);
    }

    @Override
    public void clear() {
      archive = new ArrayList<>(this);
      clear();
    }
  }
}
