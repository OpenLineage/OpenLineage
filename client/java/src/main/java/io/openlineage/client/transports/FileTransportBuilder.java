/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class FileTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new FileConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new FileTransport((FileConfig) config);
  }

  @Override
  public String getType() {
    return "file";
  }
}
