package io.openlineage.spark.extension.v1;

public interface FieldWithExprId extends DatasetFieldLineage {
  String getField();

  OlExprId getExprId();
}
