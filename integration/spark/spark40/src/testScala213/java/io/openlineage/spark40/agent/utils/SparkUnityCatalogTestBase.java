/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import io.unitycatalog.client.model.CreateCatalog;
import io.unitycatalog.server.utils.ServerProperties;
import java.net.InetAddress;
import java.util.Map;
import java.util.Properties;
import org.apache.spark.sql.SparkSession;

/**
 * Base class for Spark integration tests that need open source Unity Catalog.
 *
 * <p>Provides a {@link SparkSession} configured with Delta Lake and an embedded Unity Catalog
 * server, exposing only the Spark + Delta + Unity Catalog wiring needed by the integration tests.
 *
 * <p>Subclasses should call {@link #createWithCatalog(String, Map)} once (e.g. in a
 * {@code @BeforeAll} hook) and {@link #cleanUpSession()} afterwards.
 */
// Abstract base for the Unity Catalog Spark test classes; it has no abstract methods of its own —
// subclasses just reuse its concrete wiring — so the PMD design rule does not apply here.
@SuppressWarnings("PMD.AbstractClassWithoutAbstractMethod")
public abstract class SparkUnityCatalogTestBase {

  protected final UcServer unityCatalogServer = UcServer.INSTANCE;

  protected SparkSession spark;

  private StorageBackend storageBackend;
  protected ExternalTablesManager externalTablesManager;

  /**
   * The storage backend for test table locations. Defaults to the local filesystem; cloud tests
   * override this (e.g. to return an {@link AbfsStorageBackend}). Invoked after the SparkSession is
   * built, so an overriding backend may capture the session's configuration.
   */
  protected StorageBackend createStorageBackend() {
    return new LocalStorageBackend();
  }

  /**
   * {@link ServerProperties} for the embedded Unity Catalog server — e.g. cloud storage credentials
   * so the server can vend temporary credentials for external locations. Defaults to empty (the
   * server defaults, used by local-filesystem tests); cloud-backed subclasses override this.
   */
  protected ServerProperties unityCatalogServerProperties() {
    return new ServerProperties(new Properties());
  }

  /**
   * Creates (server-side) the given catalog and returns a {@link SparkSession} wired to talk to the
   * embedded Unity Catalog server for it, with Delta Lake enabled. {@code extraConfig} is applied
   * on top of the base configuration (e.g. cloud storage credentials for an abfss-backed catalog).
   *
   * @param catalogName the Unity Catalog catalog to create and configure
   * @param extraConfig additional Spark configuration entries to apply
   * @return the configured (and cached) SparkSession
   */
  protected SparkSession createWithCatalog(String catalogName, Map<String, String> extraConfig) {
    unityCatalogServer.start(unityCatalogServerProperties());
    createCatalog(catalogName);

    String unityCatalogConfBase = "spark.sql.catalog." + catalogName;
    SparkSession.Builder builder =
        SparkSession.builder()
            .master("local[*]")
            // Bind the driver to loopback to avoid flaky binding on multi-interface/CI machines.
            .config("spark.driver.host", InetAddress.getLoopbackAddress().getHostAddress())
            // Disable the UI to avoid port conflicts in parallel runs.
            .config("spark.ui.enabled", "false")
            // Delta SQL extensions are required, also by Unity Catalog.
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            // Unity Catalog wiring.
            .config(unityCatalogConfBase, "io.unitycatalog.spark.UCSingleCatalog")
            .config(unityCatalogConfBase + ".uri", unityCatalogServer.getUri().toString())
            // Presence of token config is required, but the value can be anything (even empty
            // string)
            .config(unityCatalogConfBase + ".token", "");

    extraConfig.forEach(builder::config);

    spark = builder.appName(getClass().getCanonicalName()).getOrCreate();
    storageBackend = createStorageBackend();
    externalTablesManager = new ExternalTablesManager(storageBackend);
    return spark;
  }

  private void createCatalog(String catalogName) {
    try {
      unityCatalogServer
          .catalogsApi()
          .createCatalog(new CreateCatalog().name(catalogName).comment("Test catalog"));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create Unity Catalog catalog: " + catalogName, e);
    }
  }

  /**
   * Removes test storage (deleting any allocated table locations) and clears the active/default
   * session references. The storage backend is closed first, while the shared SparkContext is still
   * alive — a cloud backend needs it to delete remote data. We deliberately do not stop the
   * SparkContext, as it is shared across suites in the same JVM and is cleaned up by a shutdown
   * hook.
   */
  protected void cleanUpSession() {
    if (storageBackend != null) {
      storageBackend.close();
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }
}
