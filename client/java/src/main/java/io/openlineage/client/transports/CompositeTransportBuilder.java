/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class CompositeTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new CompositeConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new CompositeTransport((CompositeConfig) config);
  }

  @Override
  public String getType() {
    return "composite";
  }
}
