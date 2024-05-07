package io.openlineage.spark.extension.v1;

import java.util.Objects;

public class InputDatasetFieldFromDelegate implements DatasetFieldLineage {

  private final Object delegate;

  private final String field;

  public InputDatasetFieldFromDelegate(Object delegate, String field) {
    this.delegate = delegate;
    this.field = field;
  }

  @Override
  public String getField() {
    return field;
  }

  public Object getDelegate() {
    return delegate;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    InputDatasetFieldFromDelegate that = (InputDatasetFieldFromDelegate) o;
    return Objects.equals(delegate, that.delegate) && Objects.equals(field, that.field);
  }

  @Override
  public int hashCode() {
    return Objects.hash(delegate, field);
  }
}
