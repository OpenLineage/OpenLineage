package io.openlineage.client.python;

public class ParameterSpec implements Dump {
  public String type;
  public String name;

  @Override
  public String dump(int nextLevel) {
    return String.format("%s %s", type, name);
  }
}
