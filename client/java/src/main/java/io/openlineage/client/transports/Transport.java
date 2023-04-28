/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
public abstract class Transport {
  enum Type {
    CONSOLE,
    HTTP,
    KAFKA,
    KINESIS,
    NOOP
  };

  @SuppressWarnings("PMD") // unused constructor type used for @NonNull validation
  Transport(@NonNull final Type type) {}

  public abstract void emit(OpenLineage.RunEvent runEvent);
}
