package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class VisitorFactoryImpl implements VisitorFactory {

  @Override
  public List<Visitor<OpenLineage.InputDataset>> getInputVisitors(OpenLineageContext context) {
    return Arrays.asList(new KafkaSourceVisitor(context), new IcebergSourceVisitor(context));
  }

  @Override
  public List<Visitor<OpenLineage.OutputDataset>> getOutputVisitors(OpenLineageContext context) {
    return Collections.singletonList(new KafkaSinkVisitor(context));
  }
}
