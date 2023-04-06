package io.openlineage.client.python;

import static io.openlineage.client.python.Utils.nestString;

import java.util.List;
import lombok.Builder;
import lombok.Getter;
import lombok.Singular;

@Getter
@Builder
public class MethodSpec implements Dump {
  public String code;
  @Singular public List<ParameterSpec> params;
  public String name;
  public String codeBlock;

  @Override
  public String dump(int nextLevel) {
    StringBuilder content = new StringBuilder();
    StringBuilder paramsList = new StringBuilder();
    boolean isFirst = true;
    for (ParameterSpec param : params) {
      if (isFirst) {
        isFirst = false;
      } else {
        paramsList.append(",");
      }
      paramsList.append(param.dump(0));
    }
    content.append(nestString(String.format("def %s(%s):", name, paramsList), 0));
    return content.toString();
  }
}
