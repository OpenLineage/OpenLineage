/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class HttpTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new HttpConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new HttpTransport((HttpConfig) config);
  }

  @Override
  public String getType() {
    return "http";
  }
}