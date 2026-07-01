/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import io.unitycatalog.client.ApiClient;
import io.unitycatalog.client.api.CatalogsApi;
import io.unitycatalog.server.UnityCatalogServer;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;

/**
 * Unity Catalog server wrapper for testing.
 *
 * <p>Manages the lifecycle of an embedded Unity Catalog server on a dynamically allocated port (to
 * avoid conflicts in parallel test execution). The server starts on first {@link #start()} and is
 * stopped automatically when the JVM exits via a shutdown hook.
 */
public final class UcServer {

  public static final UcServer INSTANCE = new UcServer();

  private final AtomicBoolean started;
  private final ReservedPort serverPort;

  @Getter private final URI uri;

  private CatalogsApi catalogsApiInstance;

  private UcServer() {
    this.started = new AtomicBoolean(false);
    this.serverPort = ReservedPort.reserveTcpPort();
    this.uri = URI.create("http://localhost:" + serverPort.getPort());
  }

  /**
   * Starts the Unity Catalog server (if not already started) with the given {@link
   * ServerProperties} and registers a shutdown hook to stop it on JVM exit. Cloud-backed tests pass
   * storage credentials here so the server can vend temporary credentials; pass empty properties
   * for the server defaults.
   */
  public void start(ServerProperties serverProperties) {
    if (started.compareAndSet(false, true)) {
      UnityCatalogServer server =
          UnityCatalogServer.builder()
              .port(serverPort.use())
              .serverProperties(serverProperties)
              .build();
      server.start();
      Runtime.getRuntime().addShutdownHook(new Thread(server::stop));
    }
  }

  public CatalogsApi catalogsApi() {
    if (catalogsApiInstance == null) {
      catalogsApiInstance =
          new CatalogsApi(
              new ApiClient()
                  .setHost(uri.getHost())
                  .setPort(uri.getPort())
                  .setScheme(uri.getScheme()));
    }
    return catalogsApiInstance;
  }
}
