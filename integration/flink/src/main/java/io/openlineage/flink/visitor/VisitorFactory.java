package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.List;

public interface VisitorFactory {

  List<Visitor<OpenLineage.InputDataset>> getInputVisitors(OpenLineageContext context);

  List<Visitor<OpenLineage.OutputDataset>> getOutputVisitors(OpenLineageContext context);
}
