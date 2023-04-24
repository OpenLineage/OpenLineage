/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util.customTransport;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.Transport;
import lombok.Getter;
import lombok.NonNull;

import java.io.Closeable;
import java.io.IOException;

@Getter
public final class TestTransport extends Transport implements Closeable {

  public TestTransport(@NonNull final TestTransportConfig testConfig) {
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {
  }

  @Override
  public void close() throws IOException {
  }
}
