/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.s3;

import io.openlineage.client.OpenLineageClientException;
import io.openlineage.client.transports.Transport;
import io.openlineage.client.transports.TransportBuilder;
import io.openlineage.client.transports.TransportConfig;

public class S3TransportBuilder implements TransportBuilder {
  @Override
  public String getType() {
    return "s3";
  }

  @Override
  public TransportConfig getConfig() {
    return new S3TransportConfig();
  }

  @Override
  public Transport build(TransportConfig config) {
    try {
      return new S3Transport((S3TransportConfig) config);
    } catch (Exception e) {
      throw new OpenLineageClientException(
          "Failed to create S3 transport with config: " + config.toString(), e);
    }
  }
}
