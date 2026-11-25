/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.client.utils.FabricWarehouseUtils;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import lombok.Value;

/**
 * The settings Fabric adds to a notebook's Spark session, such as its workspace and default
 * lakehouse. All Fabric code reads them through this class. Outside Fabric, every field is null.
 */
@Value
public class FabricSession {
  public static final String TENANT_ID_CONF = "trident.tenant.id";
  public static final String WORKSPACE_ID_CONF = "trident.workspace.id";
  public static final String WORKSPACE_NAME_CONF = "trident.workspace.name";
  public static final String LAKEHOUSE_ID_CONF = "trident.lakehouse.id";
  public static final String LAKEHOUSE_NAME_CONF = "trident.lakehouse.name";

  /** The tenant (organization) the workspace belongs to. */
  UUID tenantId;

  /** The workspace the session runs in. */
  UUID workspaceId;

  String workspaceName;

  /** The notebook's default lakehouse. */
  UUID lakehouseId;

  String lakehouseName;

  /**
   * Reads the settings. Invalid IDs and blank names are left out.
   *
   * @param conf looks up a Spark setting; see {@link FabricWarehouseSparkUtils#confOf}
   */
  public static FabricSession of(Function<String, Optional<String>> conf) {
    return new FabricSession(
        guid(conf, TENANT_ID_CONF),
        guid(conf, WORKSPACE_ID_CONF),
        name(conf, WORKSPACE_NAME_CONF),
        guid(conf, LAKEHOUSE_ID_CONF),
        name(conf, LAKEHOUSE_NAME_CONF));
  }

  /** Whether the session runs in Fabric, judged by whether its workspace ID is set. */
  public boolean isFabric() {
    return workspaceId != null;
  }

  private static UUID guid(Function<String, Optional<String>> conf, String key) {
    return conf.apply(key).flatMap(FabricWarehouseUtils::parseGuid).orElse(null);
  }

  private static String name(Function<String, Optional<String>> conf, String key) {
    return conf.apply(key).map(String::trim).filter(v -> !v.isEmpty()).orElse(null);
  }
}
