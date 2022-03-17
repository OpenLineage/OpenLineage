package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.List;
import lombok.NonNull;

public abstract class Visitor<D extends OpenLineage.Dataset> {
  @NonNull protected final OpenLineageContext context;

  public Visitor(@NonNull OpenLineageContext context) {
    this.context = context;
  }

  protected DatasetFactory<OpenLineage.OutputDataset> outputDataset() {
    return DatasetFactory.output(context.getOpenLineage());
  }

  protected DatasetFactory<OpenLineage.InputDataset> inputDataset() {
    return DatasetFactory.input(context.getOpenLineage());
  }

  public abstract boolean isDefinedAt(Object object);

  public abstract List<D> apply(Object object);
}
