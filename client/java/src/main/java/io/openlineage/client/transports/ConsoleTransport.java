/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConsoleTransport extends Transport {
  public ConsoleTransport() {
    super(Type.CONSOLE);
  }

  @Override
  public void emit(OpenLineage.RunEvent runEvent) {
    // if DEBUG loglevel is enabled, this will double-log even due to OpenLineageClient also logging
    log.info(OpenLineageClientUtils.toJson(runEvent));
  }

  @Override
  public void emit(String eventJson) {
    // if DEBUG loglevel is enabled, this will double-log even due to OpenLineageClient also logging
    log.info(eventJson);
  }
}
