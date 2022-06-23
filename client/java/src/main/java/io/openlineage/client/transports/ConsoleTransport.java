/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.Utils;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class ConsoleTransport extends Transport {
  public ConsoleTransport() {
    super(Type.CONSOLE);
  }

  @Override
  public void emit(OpenLineage.RunEvent runEvent) {
    log.info(Utils.toJson(runEvent));
  }
}
