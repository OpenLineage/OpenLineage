/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
public abstract class Transport implements AutoCloseable {
  public abstract void emit(@NonNull OpenLineage.RunEvent runEvent);

  public abstract void emit(@NonNull OpenLineage.DatasetEvent datasetEvent);

  public abstract void emit(@NonNull OpenLineage.JobEvent jobEvent);

  @Override
  public void close() throws Exception {}
}
