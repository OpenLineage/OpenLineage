package io.openlineage.client.python;

import static io.openlineage.client.python.Utils.nestString;

import java.util.Map;
import lombok.Builder;

@Builder
public class FieldSpec {
  public TypeRef type;
  public String name;
  public String defaultValue;

  public String dump(int nestLevel, Map<TypeRef, TypeRef> parentClassMapping) {
    String resultType = "";
    String resultValue = "";

    if (type != null) {
      if (parentClassMapping.containsKey(type)) {
        TypeRef parent = parentClassMapping.get(type);
        if (parent != null) {
          resultType = String.format(": %s", parent.getName());
        } else {
          resultType = ": dict";
        }
      } else {
        resultType = String.format(": %s", type.getName());
      }
    }

    if (defaultValue != null) {
      resultValue = String.format(" = %s", defaultValue);
    }
    return nestString(String.format("%s%s%s", name, resultType, resultValue), nestLevel);
  }
}
