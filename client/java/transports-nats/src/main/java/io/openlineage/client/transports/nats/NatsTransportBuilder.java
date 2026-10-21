/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;

public class NatsTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new NatsConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new NatsTransport((NatsConfig) config);
  }

  @Override
  public String getType() {
    return "nats";
  }
}
