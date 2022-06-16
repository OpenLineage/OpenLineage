/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

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

  private final Type type;

  Transport(@NonNull final Type type) {
    this.type = type;
  }

  public abstract void emit(OpenLineage.RunEvent runEvent);
}
