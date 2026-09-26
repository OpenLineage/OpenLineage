/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.SocketAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;

/**
 * A {@link java.net.Socket} that ignores the TCP endpoint it is asked to connect to and instead
 * dials a Unix Domain Socket at a fixed path. This lets the blocking Apache HttpClient, which only
 * knows how to talk to {@code host:port}, tunnel its requests over a UDS, e.g. a socket endpoint
 * exposed on a Kubernetes node.
 *
 * <p>Backed by the JDK-native Unix Domain Socket support introduced in Java 16 (JEP 380) rather
 * than a third-party library, so it adds no dependencies. It references Java 16+ types directly, so
 * {@link HttpTransport} guards construction with a runtime version check: this class may be loaded
 * on an older JVM (the Java 16+ types it names are only symbolic references, resolved lazily), but
 * its methods must never execute there — the version check ensures they don't, avoiding a {@link
 * NoClassDefFoundError} on the Java 16+ types. The client is compiled with {@code source/target 8}
 * against a JDK 17 toolchain, so this compiles to Java 8 bytecode while still referencing the newer
 * APIs.
 *
 * <p>Because the HTTP exchange runs over channel-backed streams, {@code SO_TIMEOUT} and other TCP
 * socket options do not apply and are accepted as no-ops. Read/write operations block; the local
 * socket makes this acceptable for best-effort lineage emission.
 */
final class TunnelingUnixSocket extends java.net.Socket {
  private final UnixDomainSocketAddress address;
  private final SocketChannel channel;
  private int soTimeout;

  TunnelingUnixSocket(File path) throws IOException {
    this.address = UnixDomainSocketAddress.of(path.toPath());
    this.channel = SocketChannel.open(StandardProtocolFamily.UNIX);
  }

  @Override
  public void connect(SocketAddress endpoint) throws IOException {
    channel.connect(address);
  }

  @Override
  public void connect(SocketAddress endpoint, int timeout) throws IOException {
    // The channel is in blocking mode; connecting to a local UDS returns as soon as the peer
    // accepts, so the (TCP-oriented) connect timeout does not apply.
    channel.connect(address);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    return Channels.newInputStream(channel);
  }

  @Override
  public OutputStream getOutputStream() throws IOException {
    return Channels.newOutputStream(channel);
  }

  @Override
  public boolean isConnected() {
    return channel.isConnected();
  }

  @Override
  public boolean isClosed() {
    return !channel.isOpen();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  @Override
  public void shutdownInput() throws IOException {
    channel.shutdownInput();
  }

  @Override
  public void shutdownOutput() throws IOException {
    channel.shutdownOutput();
  }

  @Override
  public SocketAddress getRemoteSocketAddress() {
    return address;
  }

  // The remaining overrides make the TCP-oriented socket options HttpClient sets during connection
  // setup into no-ops (or harmless getters); the underlying default Socket impl is never used, so
  // touching it would otherwise throw because this socket is never truly "created".
  @Override
  public void setSoTimeout(int timeout) {
    this.soTimeout = timeout;
  }

  @Override
  public int getSoTimeout() {
    return soTimeout;
  }

  @Override
  public void setTcpNoDelay(boolean on) {
    // no-op: not applicable to a Unix Domain Socket
  }

  @Override
  public boolean getTcpNoDelay() {
    return true;
  }

  @Override
  public void setKeepAlive(boolean on) {
    // no-op: not applicable to a Unix Domain Socket
  }

  @Override
  public boolean getKeepAlive() {
    return false;
  }

  @Override
  public void setReuseAddress(boolean on) {
    // no-op: not applicable to a Unix Domain Socket
  }

  @Override
  public boolean getReuseAddress() {
    return false;
  }

  @Override
  public void setSoLinger(boolean on, int linger) {
    // no-op: not applicable to a Unix Domain Socket
  }

  @Override
  public int getSoLinger() {
    return -1;
  }
}
