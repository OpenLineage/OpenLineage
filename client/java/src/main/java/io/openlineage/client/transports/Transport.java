package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;

public abstract class Transport {
  enum Type {
    CONSOLE,
    HTTP,
    KAFKA,
    NOOP
  };

  @SuppressWarnings("PMD") // unused constructor type used for @NonNull validation
  Transport(@NonNull final Type type) {}

  public abstract void emit(OpenLineage.RunEvent runEvent);
}
