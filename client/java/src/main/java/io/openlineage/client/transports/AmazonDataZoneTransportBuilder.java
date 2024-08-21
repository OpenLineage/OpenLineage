/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class AmazonDataZoneTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new AmazonDataZoneConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new AmazonDataZoneTransport((AmazonDataZoneConfig) config);
  }

  @Override
  public String getType() {
    return "amazon_datazone";
  }
}
