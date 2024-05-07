package io.openlineage.spark.extension.v1;

import io.openlineage.client.utils.DatasetIdentifier;
import java.util.Objects;

public class InputDatasetFieldWithIdentifier implements DatasetFieldLineage, FieldWithExprId {

  private final DatasetIdentifier datasetIdentifier;
  private final String field;

  private final OlExprId exprId;

  public InputDatasetFieldWithIdentifier(
      DatasetIdentifier datasetIdentifier, String field, OlExprId exprId) {
    this.datasetIdentifier = datasetIdentifier;
    this.field = field;
    this.exprId = exprId;
  }

  @Override
  public String getField() {
    return field;
  }

  @Override
  public OlExprId getExprId() {
    return exprId;
  }

  public DatasetIdentifier getDatasetIdentifier() {
    return datasetIdentifier;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputDatasetFieldWithIdentifier that = (InputDatasetFieldWithIdentifier) o;
    return Objects.equals(datasetIdentifier, that.datasetIdentifier)
        && Objects.equals(field, that.field)
        && Objects.equals(exprId, that.exprId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(datasetIdentifier, field, exprId);
  }
}
