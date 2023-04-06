package io.openlineage.client.python;

import static io.openlineage.client.python.Utils.nestString;

import lombok.Builder;

@Builder
public class FieldSpec implements Dump {
  public TypeRef type;
  public String name;
  public String defaultValue;

  @Override
  public String dump(int nestLevel) {
    String resultType = "";
    String resultValue = "";

    if (type != null) {
      resultType = String.format(": %s", type.getName());
    }

    if (defaultValue != null) {
      resultValue = String.format(" = %s", defaultValue);
    }
    return nestString(String.format("%s%s%s", name, resultType, resultValue), nestLevel);
  }
}
