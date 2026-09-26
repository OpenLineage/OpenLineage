/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import io.unitycatalog.server.utils.ServerProperties;
import java.net.URI;
import java.util.Properties;
import org.junit.jupiter.api.Tag;

/**
 * Base for Unity Catalog table tests that run against Azure ADLS Gen2 (abfss). Provisions tables
 * under the configured writable container ({@link AzureDynamicParameter#ContainerUri}). All
 * credentials come from {@link AzureDynamicParameter}: the embedded Unity Catalog server vends
 * per-table user-delegation SAS (see {@link #unityCatalogServerProperties()}), so the SparkSession
 * itself carries no storage credentials. Tagged {@code azure} (inherited by subclasses), so these
 * run only with {@code -Pazure}.
 */
@Tag("azure")
public abstract class AbfssUnityCatalogTestBase extends UnityCatalogTableTestBase {

  private static final String ADLS_HOST_SUFFIX = ".dfs.core.windows.net";

  private AbfsStorageConfig storageConfig;

  @Override
  protected StorageBackend createStorageBackend() {
    return new AbfsStorageBackend(azureContainerUri(), azureStorageConfig());
  }

  /**
   * Registers the abfss service principal with the embedded Unity Catalog server so it mints
   * (vends) user-delegation SAS (shared access signature) tokens for the test tables' storage
   * account. The connector then uses those vended tokens. Built from the required {@link
   * AzureDynamicParameter} service principal (a missing value fails fast).
   */
  @Override
  protected ServerProperties unityCatalogServerProperties() {
    Properties properties = new Properties();
    properties.setProperty("adls.storageAccountName.0", azureStorageAccount());
    properties.setProperty("adls.tenantId.0", AzureDynamicParameter.TenantId.resolve());
    properties.setProperty("adls.clientId.0", AzureDynamicParameter.ClientId.resolve());
    properties.setProperty("adls.clientSecret.0", AzureDynamicParameter.ClientSecret.resolve());
    return new ServerProperties(properties);
  }

  /** The configured writable abfss container base. Fails fast if unset. */
  private String azureContainerUri() {
    return AzureDynamicParameter.ContainerUri.resolve();
  }

  /** The storage account name, parsed from the container URI's authority. */
  private String azureStorageAccount() {
    String host = URI.create(azureContainerUri()).getHost();
    return host.endsWith(ADLS_HOST_SUFFIX)
        ? host.substring(0, host.length() - ADLS_HOST_SUFFIX.length())
        : host;
  }

  /**
   * Credentials for direct Hadoop access by {@link AbfsStorageBackend} (run-root cleanup), resolved
   * once and memoized. When no key or service principal is configured, the backend's ABFS driver
   * falls back to the environment's ambient credentials.
   */
  private AbfsStorageConfig azureStorageConfig() {
    if (storageConfig == null) {
      storageConfig =
          AbfsStorageConfig.builder()
              .account(azureStorageAccount())
              .clientId(AzureDynamicParameter.ClientId.resolve())
              .clientSecret(AzureDynamicParameter.ClientSecret.resolve())
              .tenantId(AzureDynamicParameter.TenantId.resolve())
              .build();
    }
    return storageConfig;
  }
}
