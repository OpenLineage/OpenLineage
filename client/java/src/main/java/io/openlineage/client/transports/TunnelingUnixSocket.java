/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.io.File;
import java.io.IOException;
import java.net.SocketAddress;
import jnr.unixsocket.UnixSocket;
import jnr.unixsocket.UnixSocketAddress;
import jnr.unixsocket.UnixSocketChannel;

/**
 * A {@link java.net.Socket} that ignores the TCP endpoint it is asked to connect to and instead
 * dials a Unix Domain Socket at a fixed path. This lets an HTTP client that only knows how to talk
 * to {@code host:port} tunnel its requests over a UDS, e.g. a socket endpoint exposed on a
 * Kubernetes node.
 *
 * <p>Adapted from OkHttp's unix-domain-sockets sample. Uses jnr-unixsocket so it works on Java 8+.
 */
final class TunnelingUnixSocket extends UnixSocket {
  private final File path;

  TunnelingUnixSocket(File path, UnixSocketChannel channel) {
    super(channel);
    this.path = path;
  }

  @Override
  public void connect(SocketAddress endpoint) throws IOException {
    super.connect(new UnixSocketAddress(path), 0);
  }

  @Override
  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    super.connect(new UnixSocketAddress(path), timeout);
  }
}
