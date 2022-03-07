package io.openlineage.flink;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.Visitor;
import io.openlineage.flink.visitor.VisitorFactory;
import io.openlineage.flink.visitor.VisitorFactoryImpl;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import javax.annotation.Nullable;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Slf4j
public class OpenLineageFlinkJobListener implements JobListener {

    private final StreamExecutionEnvironment executionEnvironment;
    private final OpenLineageContext openLineageContext;

    public OpenLineageFlinkJobListener(StreamExecutionEnvironment executionEnvironment) {
        makeTransformationsUnclearableList(executionEnvironment);
        this.executionEnvironment = executionEnvironment;
        this.openLineageContext = OpenLineageContext.builder()
                .streamExecutionEnvironment(Optional.of(executionEnvironment))
                .openLineage(new OpenLineage(URI.create("")))
                .build();
    }

    @Override
    public void onJobSubmitted(@Nullable JobClient jobClient, @Nullable Throwable throwable) {
        if (jobClient != null) {
            log.info("JobId: {}", jobClient.getJobID());
            Field transformationsField = FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
            try {
                List<Transformation<?>> transformations = (List<Transformation<?>>) transformationsField.get(executionEnvironment);
                VisitorFactory visitorFactory = new VisitorFactoryImpl();

                List<OpenLineage.InputDataset> inputDatasets = getInputDatasets(visitorFactory, transformations);
                List<OpenLineage.OutputDataset> outputDatasets = getOutputDatasets(visitorFactory, transformations);

                createRunEventAndEmit(inputDatasets, outputDatasets);
            } catch (IllegalAccessException e) {
                log.error("Can't access the field. ", e);
            }
        }
    }

    private void createRunEventAndEmit(List<OpenLineage.InputDataset> inputDatasets, List<OpenLineage.OutputDataset> outputDatasets) {
        OpenLineage.RunEvent runEvent = openLineageContext
                .getOpenLineage()
                .newRunEventBuilder()
                .inputs(inputDatasets)
                .outputs(outputDatasets).build();

        log.info("RunEvent: {}", runEvent);
    }

    private List<OpenLineage.InputDataset> getInputDatasets(VisitorFactory visitorFactory, List<Transformation<?>> transformations) {
        List<OpenLineage.InputDataset> inputDataSets = new ArrayList<>();
        List<Visitor<Transformation<?>, OpenLineage.InputDataset>> inputVisitors = visitorFactory.getInputVisitors(openLineageContext);

        for (Transformation<?> transformation : transformations) {
            inputDataSets.addAll(inputVisitors.stream()
                    .filter(inputVisitor -> inputVisitor.isDefinedAt(transformation))
                    .map(inputVisitor -> inputVisitor.apply(transformation))
                    .flatMap(List::stream)
                    .collect(Collectors.toList()));
        }
        return inputDataSets;
    }

    private List<OpenLineage.OutputDataset> getOutputDatasets(VisitorFactory visitorFactory, List<Transformation<?>> transformations) {
        List<OpenLineage.OutputDataset> outputDatasets = new ArrayList<>();
        List<Visitor<Transformation<?>, OpenLineage.OutputDataset>> inputVisitors = visitorFactory.getOutputVisitors(openLineageContext);

        for (Transformation<?> transformation : transformations) {
            outputDatasets.addAll(inputVisitors.stream()
                    .filter(inputVisitor -> inputVisitor.isDefinedAt(transformation))
                    .map(inputVisitor -> inputVisitor.apply(transformation))
                    .flatMap(List::stream)
                    .collect(Collectors.toList()));
        }
        return outputDatasets;
    }

    @Override
    public void onJobExecuted(@Nullable JobExecutionResult jobExecutionResult, @Nullable Throwable throwable) {
        log.info("JobExecutionResult: {}", jobExecutionResult);
    }

    private void makeTransformationsUnclearableList(StreamExecutionEnvironment executionEnvironment) {
        try {
            Field transformations = FieldUtils.getField(StreamExecutionEnvironment.class, "transformations", true);
            ArrayList<Transformation<?>> previousTransformationList = (ArrayList<Transformation<?>>) FieldUtils.readField(transformations, executionEnvironment, true);
            List<Transformation<?>> transformationList = new UnclearableList<>(previousTransformationList);
            FieldUtils.writeField(transformations, executionEnvironment, transformationList, true);
        } catch (IllegalAccessException e) {
            log.error("Failed to rewrite transformations");
        }
    }

    static class UnclearableList<T> extends ArrayList<T> {
        public UnclearableList(Collection<T> collection) {
            super(collection);
            log.info("Created un-clearableList");
        }
        @Override
        public void clear() {
            log.info("Trying to clear un-clearable list");
        }
    }
}