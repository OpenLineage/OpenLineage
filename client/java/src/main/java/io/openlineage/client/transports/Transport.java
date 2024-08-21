/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
    FILE,
    HTTP,
    KAFKA,
    KINESIS,
    NOOP,
    AMAZON_DATAZONE
  };

  @SuppressWarnings("PMD") // unused constructor type used for @NonNull validation
  Transport(@NonNull final Type type) {}

  public abstract void emit(@NonNull OpenLineage.RunEvent runEvent);

  public abstract void emit(@NonNull OpenLineage.DatasetEvent datasetEvent);

  public abstract void emit(@NonNull OpenLineage.JobEvent jobEvent);
}
