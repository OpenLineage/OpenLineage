/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConsoleTransport extends Transport {
  public ConsoleTransport() {
    super(Type.CONSOLE);
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
    emit(OpenLineageClientUtils.toJson(runEvent));
  }

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {
    emit(OpenLineageClientUtils.toJson(datasetEvent));
  }

  @Override
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
  @Override
  public void emit(String eventJson) {
    // if DEBUG loglevel is enabled, this will double-log even due to OpenLineageClient also logging
    log.info(eventJson);
  }
}
