/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark40.agent.utils;

import java.util.HashMap;
import java.util.Map;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * OAuth service-principal credentials (client id, client secret and tenant id) for one Azure ADLS
 * Gen2 storage account ({@code account.dfs.core.windows.net}). Used in {@link AbfsStorageBackend}
 * cleanup
 */
@Value
@Builder
public class AbfsStorageConfig {
  @NonNull String account;
  String clientId;
  String clientSecret;
  String tenantId;

  /** The Hadoop {@code fs.azure.*} OAuth entries registering this account's service principal */
  public Map<String, String> toHadoopConfig() {
    Map<String, String> config = new HashMap<>();
    String accountSuffix = account + ".dfs.core.windows.net";
    config.put("fs.azure.account.auth.type." + accountSuffix, "OAuth");
    config.put(
        "fs.azure.account.oauth.provider.type." + accountSuffix,
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
    config.put("fs.azure.account.oauth2.client.id." + accountSuffix, clientId);
    config.put("fs.azure.account.oauth2.client.secret." + accountSuffix, clientSecret);
    config.put(
        "fs.azure.account.oauth2.client.endpoint." + accountSuffix,
        "https://login.microsoftonline.com/" + tenantId + "/oauth2/token");
    return config;
  }
}
