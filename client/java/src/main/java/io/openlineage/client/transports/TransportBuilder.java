/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

public interface TransportBuilder {
  String getType();

  TransportConfig getConfig();

  Transport build(TransportConfig config);
}
