package io.openlineage.client.python;

import lombok.Builder;

@Builder
public class DecoratorSpec {
  public String name;

  public String dump(int nestLevel) {
    return String.format("@%s", name);
  }
}
