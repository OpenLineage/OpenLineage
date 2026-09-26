/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import java.time.Duration;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
public abstract class Transport implements AutoCloseable {
  public abstract void emit(@NonNull OpenLineage.RunEvent runEvent);

  public abstract void emit(@NonNull OpenLineage.DatasetEvent datasetEvent);

  public abstract void emit(@NonNull OpenLineage.JobEvent jobEvent);

  /**
   * Emit the given run event with a bounded wait when supported by the transport.
   *
   * <p>The default implementation keeps regular emit behavior for transports that do not support
   * delivery acknowledgement.
   */
  public void emit(@NonNull OpenLineage.RunEvent runEvent, @NonNull Duration timeout) {
    emit(runEvent);
  }

  @Override
  public void close() throws Exception {}
}
