/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public class KafkaTransportBuilder implements TransportBuilder {

  @Override
  public TransportConfig getConfig() {
    return new KafkaConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    return new KafkaTransport((KafkaConfig) config);
  }

  @Override
  public String getType() {
    return "kafka";
  }
}
