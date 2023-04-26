/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

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
