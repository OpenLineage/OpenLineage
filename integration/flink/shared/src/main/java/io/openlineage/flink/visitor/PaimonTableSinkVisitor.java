package io.openlineage.flink.visitor;

import static io.openlineage.flink.visitor.wrapper.PaimonTableSinkWrapper.getFullTableName;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.visitor.wrapper.PaimonTableSinkWrapper;
import io.openlineage.flink.visitor.wrapper.WrapperUtils;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.Table;

@Slf4j
public class PaimonTableSinkVisitor extends Visitor<OpenLineage.OutputDataset> {
  private static final String PAIMON_SINK_OPERATOR =
      "org.apache.paimon.flink.sink.CommitterOperator";

  public PaimonTableSinkVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  @Override
  public boolean isDefinedAt(Object sink) {
    if (!(sink instanceof OneInputTransformation)) {
      return false;
    }

    try {
      OneInputTransformation<?, ?> transformation = (OneInputTransformation<?, ?>) sink;
      String operatorClassName =
          transformation
              .getOperatorFactory()
              .getStreamOperatorClass(ClassLoader.getSystemClassLoader())
              .getCanonicalName();
      return PAIMON_SINK_OPERATOR.equals(operatorClassName);
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public List<OpenLineage.OutputDataset> apply(Object paimonSink) {
    OneInputTransformation<?, ?> transformation =
        (OneInputTransformation<?, ?>)
            ((OneInputTransformation<?, ?>) paimonSink).getInputs().get(0);

    PaimonTableSinkWrapper sinkWrapper =
        PaimonTableSinkWrapper.of(transformation.getOperatorFactory());

    return sinkWrapper
        .getTable()
        .map(table -> getDataset(context, table))
        .map(dataset -> Collections.singletonList(dataset))
        .orElse(Collections.emptyList());
  }

  private OpenLineage.OutputDataset getDataset(OpenLineageContext context, Table table) {
    OpenLineage openLineage = context.getOpenLineage();
    Optional<Path> path = WrapperUtils.getFieldValue(table.getClass(), table, "path");
    String namespace = path.isPresent() ? path.get().toString() : table.name();
    String name =
        getFullTableName(table).isPresent() ? getFullTableName(table).get() : table.name();
    return openLineage
        .newOutputDatasetBuilder()
        .name(name)
        .namespace(namespace)
        //                .namespace(datasetIdentifier.getNamespace())
        //                .facets(
        //                        openLineage
        //                                .newDatasetFacetsBuilder()
        //                                .schema(Optional.empty())
        //                                .build())
        .build();
  }
}
