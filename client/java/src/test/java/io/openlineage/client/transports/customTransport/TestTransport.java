/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.customTransport;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.transports.Transport;
import java.io.Closeable;
import java.io.IOException;
import lombok.Getter;
import lombok.NonNull;

@Getter
public final class TestTransport extends Transport implements Closeable {

  private final String param1;
  private final String param2;

  public TestTransport(@NonNull final TestTransportConfig testConfig) {
    this.param1 = testConfig.getParam1();
    this.param2 = testConfig.getParam2();
  }

  @Override
  public void emit(@NonNull OpenLineage.RunEvent runEvent) {}

  @Override
  public void close() throws IOException {}
}
