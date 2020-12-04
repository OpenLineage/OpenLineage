package openlineage.api.backend;

public final class Parameter {

  private final String key;
  private final Object value;

  public Parameter(String key, Object value) {
    this.key = key;
    this.value = value;
  }
}
