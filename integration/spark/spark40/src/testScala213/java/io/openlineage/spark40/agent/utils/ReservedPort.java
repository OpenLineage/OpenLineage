/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Represents a reserved TCP port that can be released and then used by a service.
 *
 * <p>Binding a {@link ServerSocket} first guarantees the port is free; releasing it just before the
 * service binds minimises the race window that causes "address already in use" errors in tests.
 */
@AllArgsConstructor
public class ReservedPort implements AutoCloseable {

  @Getter final int port;
  private ServerSocket socket;

  /**
   * Releases the port reservation and returns the port number so the actual service can bind to it.
   */
  public int use() {
    close();
    return port;
  }

  @Override
  public void close() {
    try {
      socket.close();
    } catch (IOException e) {
      throw new RuntimeException("Failed to release reserved port " + port, e);
    }
  }

  /** Reserves a random available TCP port on the loopback interface. */
  public static ReservedPort reserveTcpPort() {
    try {
      ServerSocket s = new ServerSocket();
      s.setReuseAddress(false);
      s.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
      return new ReservedPort(s.getLocalPort(), s);
    } catch (IOException e) {
      throw new RuntimeException("Failed to reserve a TCP port", e);
    }
  }
}
