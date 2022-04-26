package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;

public abstract class Transport {
  enum Type {
    CONSOLE,
    HTTP,
    KAFKA
  };

  private final Type type;

  Transport(@NonNull final Type type) {
    this.type = type;
  }

  public abstract void emit(OpenLineage.RunEvent runEvent);
}
