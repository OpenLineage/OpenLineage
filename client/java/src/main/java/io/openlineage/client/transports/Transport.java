/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
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
    NOOP
  };

  @SuppressWarnings("PMD") // unused constructor type used for @NonNull validation
  Transport(@NonNull final Type type) {}

  public abstract void emit(@NonNull OpenLineage.RunEvent runEvent);

  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emit(OpenLineageClientUtils.toJson(datasetEvent));
  }

  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {
    emit(OpenLineageClientUtils.toJson(jobEvent));
  }

  /**
   * @deprecated
   *     <p>Since version 1.13.0.
   *     <p>Will be removed in version 1.16.0.
   *     <p>Please use {@link #emit(OpenLineage.DatasetEvent)} or {@link
   *     #emit(OpenLineage.JobEvent)} instead
   */
  @Deprecated
  public void emit(String eventAsJson) {
    throw new UnsupportedOperationException(
        "Please implement emit(OpenLineage.DatasetEvent) and emit(OpenLineage.JobEvent)");
  }
}
