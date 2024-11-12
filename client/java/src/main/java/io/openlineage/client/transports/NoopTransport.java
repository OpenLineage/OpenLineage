/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NoopTransport extends Transport {
  public NoopTransport() {
    log.info("OpenLineage client is disabled");
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {}

  @Override
  public void emit(@NonNull OpenLineage.DatasetEvent datasetEvent) {}

  @Override
  public void emit(@NonNull OpenLineage.JobEvent jobEvent) {}
}
