package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import org.apache.flink.api.dag.Transformation;

import java.util.List;

public interface VisitorFactory {

    List<Visitor<Transformation<?>, OpenLineage.InputDataset>> getInputVisitors(OpenLineageContext context);

    List<Visitor<Transformation<?>, OpenLineage.OutputDataset>> getOutputVisitors(OpenLineageContext context);
}
