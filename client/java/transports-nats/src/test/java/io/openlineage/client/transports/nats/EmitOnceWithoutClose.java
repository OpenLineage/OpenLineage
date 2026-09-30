/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports.nats;

/**
 * Emits one event and returns from main without closing the transport, the way a short-lived job
 * that forgets to close its client does. The JVM must still exit.
 */
public final class EmitOnceWithoutClose {
  private EmitOnceWithoutClose() {}

  public static void main(String[] args) {
    NatsConfig config = new NatsConfig();
    config.setUrl(args[0]);
    config.setSubject("ol.emit-once");
    config.setJetstream(false);
    new NatsTransport(config).emit(NatsServerScenariosTest.runEvent("emit-once"));
  }
}
