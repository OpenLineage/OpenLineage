/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.customTransport;

import io.openlineage.client.transports.CustomTransportBuilder;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportConfig;

public class TestTransportBuilder implements CustomTransportBuilder {
  @Override
  public TransportConfig getConfig() {
    return new TestTransportConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new TestTransport((TestTransportConfig) config);
  }

  @Override
  public String getType() {
    return "test";
  }
}
