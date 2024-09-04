/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.transports.dataplex;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;
import java.io.IOException;

public class DataplexTransportBuilder implements TransportBuilder {
  @Override
  public String getType() {
    return "dataplex";
  }

  @Override
  public TransportConfig getConfig() {
    return new DataplexConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    try {
      return new DataplexTransport((DataplexConfig) config);
    } catch (IOException e) {
      throw new OpenLineageClientException(
          "An exception occurred while creating a DataplexTransport", e);
    }
  }
}
