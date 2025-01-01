/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.gcplineage;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;
import java.io.IOException;

public class GcpLineageTransportBuilder implements TransportBuilder {
  @Override
  public String getType() {
    return "gcplineage";
  }

  @Override
  public TransportConfig getConfig() {
    return new GcpLineageTransportConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    try {
      return new GcpLineageTransport((GcpLineageTransportConfig) config);
    } catch (IOException e) {
      throw new OpenLineageClientException(
          "An exception occurred while creating a GcpLineageTransport", e);
    }
  }
}
