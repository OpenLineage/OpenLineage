package io.openlineage.spark.extension.v1;

import java.util.List;

public interface ColumnLevelLineageNode {
  List<DatasetFieldLineage> getColumnLevelLineageInputs(OpenLineageExtensionContext context);

  List<DatasetFieldLineage> getColumnLevelLineageOutputs(OpenLineageExtensionContext context);

  List<ExpressionDependency> getColumnLevelLineageDependencies(OpenLineageExtensionContext context);
}
