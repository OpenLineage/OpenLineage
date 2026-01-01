/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.kinesis;

import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;

public class KinesisTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new KinesisConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new KinesisTransport((KinesisConfig) config);
  }

  @Override
  public String getType() {
    return "kinesis";
  }
}
