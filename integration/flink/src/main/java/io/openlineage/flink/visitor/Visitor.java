package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import lombok.NonNull;
import org.apache.flink.api.dag.Transformation;

import java.util.List;

public abstract class Visitor<T extends Transformation<?>, D extends OpenLineage.Dataset> {
    @NonNull
    protected final OpenLineageContext context;

    public Visitor(@NonNull OpenLineageContext context) {
        this.context = context;
    }

    protected DatasetFactory<OpenLineage.OutputDataset> outputDataset() {
        return DatasetFactory.output(context.getOpenLineage());
    }

    protected DatasetFactory<OpenLineage.InputDataset> inputDataset() {
        return DatasetFactory.input(context.getOpenLineage());
    }

    public abstract boolean isDefinedAt(T transformation);

    public abstract List<D> apply(T t);
}
