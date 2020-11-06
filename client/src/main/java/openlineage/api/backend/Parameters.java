package openlineage.api.backend;

import java.util.ArrayList;
import java.util.List;

public class Parameters {
  private final List<Parameter> parameters = new ArrayList<Parameter>();
  public Parameters add(String key, Object value) {
    parameters.add(new Parameter(key, value));
    return this;
  }
}
