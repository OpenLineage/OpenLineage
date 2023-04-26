/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class ConsoleTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new ConsoleConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new ConsoleTransport();
  }

  @Override
  public String getType() {
    return "console";
  }
}