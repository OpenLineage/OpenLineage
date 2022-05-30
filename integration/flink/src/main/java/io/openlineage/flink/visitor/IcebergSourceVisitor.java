package io.openlineage.flink.visitor;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.DatasetIdentifier;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.PathUtils;
import io.openlineage.flink.visitor.wrapper.IcebergSourceWrapper;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.source.StreamingMonitorFunction;

@Slf4j
public class IcebergSourceVisitor extends Visitor<OpenLineage.InputDataset> {
  public IcebergSourceVisitor(@NonNull OpenLineageContext context) {
    super(context);
  }

  public static boolean hasClasses() {
    try {
      IcebergSourceVisitor.class
          .getClassLoader()
          .loadClass("org.apache.iceberg.flink.source.StreamingMonitorFunction");
      return true;
    } catch (Exception e) {
      // swallow- we don't care
    }
    return false;
  }

  @Override
  public boolean isDefinedAt(Object source) {
    return source instanceof StreamingMonitorFunction;
  }

  @Override
  public List<OpenLineage.InputDataset> apply(Object source) {
    IcebergSourceWrapper sourceWrapper = IcebergSourceWrapper.of((StreamingMonitorFunction) source);
    return Collections.singletonList(getDataset(sourceWrapper.getTable()));
  }

  private OpenLineage.SchemaDatasetFacet getSchema(Table table) {
    List<OpenLineage.SchemaDatasetFacetFields> fields =
        table.schema().columns().stream()
            .map(
                field ->
                    context
                        .getOpenLineage()
                        .newSchemaDatasetFacetFields(
                            field.name(), field.type().typeId().name(), field.doc()))
            .collect(Collectors.toList());
    return context.getOpenLineage().newSchemaDatasetFacet(fields);
  }

  private OpenLineage.InputDataset getDataset(Table table) {
    OpenLineage openLineage = context.getOpenLineage();
    DatasetIdentifier datasetIdentifier = PathUtils.fromURI(URI.create(table.location()));
    return openLineage
        .newInputDatasetBuilder()
        .name(datasetIdentifier.getName())
        .namespace(datasetIdentifier.getNamespace())
        .facets(openLineage.newDatasetFacetsBuilder().schema(getSchema(table)).build())
        .build();
  }
}
