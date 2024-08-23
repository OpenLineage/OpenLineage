/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import com.sun.net.httpserver.HttpServer;
import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

public final class StatefulHttpServer implements Closeable {
  private final HttpServer server;
  private final OpenLineageHttpHandler handler;

  private StatefulHttpServer(HttpServer server, OpenLineageHttpHandler handler) {
    this.server = server;
    this.handler = handler;
  }

  public static StatefulHttpServer create(int port, String path, OpenLineageHttpHandler handler)
      throws IOException {
    if (port < 1024 || port > 65535) {
      throw new IllegalArgumentException("Port must be between 1024 and 65535");
    }
    Objects.requireNonNull(path, "path cannot be null");
    Objects.requireNonNull(handler, "handler cannot be null");

    HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
    server.createContext(path, handler);
    server.setExecutor(null);
    return new StatefulHttpServer(server, handler);
  }

  public static StatefulHttpServer create(String path, OpenLineageHttpHandler handler)
      throws IOException {
    Random random = new Random();
    int port = random.nextInt(1024, 65535);
    return create(port, path, handler);
  }

  public List<String> events() {
    return new ArrayList<>(handler.getEvents());
  }

  public String getHost() {
    return server.getAddress().getHostString();
  }

  public int getPort() {
    return server.getAddress().getPort();
  }

  public void clear() {
    handler.clear();
  }

  public void start() {
    server.start();
  }

  @Override
  public void close() throws IOException {
    server.stop(0);
    clear();
  }
}
