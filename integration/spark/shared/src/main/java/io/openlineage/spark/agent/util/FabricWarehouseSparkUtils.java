/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.FabricWarehouseUtils;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

/**
 * Names the table written by Microsoft's Spark connector for Fabric warehouses ({@code
 * synapsesql}).
 *
 * <p>The connector writes in two steps. First, Spark saves the data as Parquet files in a temporary
 * folder. Then the connector loads those files into the warehouse table with {@code COPY INTO},
 * which runs outside Spark. OpenLineage only sees the first step, so it used to report the
 * temporary folder as the output. The real table is only in the first step's options, for example:
 *
 * <pre>
 * [WorkspaceId=8b4cb800-…, DatawarehouseId=0fd9a0c2-…,
 *  three-part_sql_table_or_view_name=wh_primary.probe_dw.a_write,
 *  path=abfss://…@spark1triprodcus.dfs.core.windows.net/user/trusted-service-user/a_write5f15…]
 * </pre>
 *
 * <p>Those options don't include the warehouse's SQL address, so it's worked out in this order:
 *
 * <ol>
 *   <li>The address the user set in Spark settings as {@code
 *       spark.datawarehouse.<warehouse>.sqlendpoint}, written as {@code host} or {@code host,port}.
 *       It is used only if it is the address of a Fabric workspace, the same check that reads use.
 *       Otherwise the write is reported with no output, without trying the next option.
 *   <li>The address built from the tenant ID ({@code trident.tenant.id}, set by Fabric) and the
 *       workspace ID. The workspace ID comes from the {@code WorkspaceId} option, or from {@code
 *       trident.workspace.id} if the option isn't set. The option comes first because it names the
 *       workspace being written to, which can differ from the notebook's own workspace in {@code
 *       trident.workspace.id}. See {@link FabricWarehouseUtils#endpointFor} for how the address is
 *       built.
 * </ol>
 *
 * <p>If no address can be worked out, the write is reported with no output. The temporary folder is
 * never reported, because it isn't where the data ends up.
 */
@Slf4j
public final class FabricWarehouseSparkUtils {
  public static final String ENDPOINT_OVERRIDE_CONF = "spark.datawarehouse.%s.sqlendpoint";
  public static final String TENANT_ID_CONF = "trident.tenant.id";
  public static final String WORKSPACE_ID_CONF = "trident.workspace.id";

  private static final int TABLE_NAME_PARTS = 3;

  private FabricWarehouseSparkUtils() {}

  /** Whether these write options come from the Fabric connector's first step. */
  public static boolean isConnectorWrite(Map<String, String> options) {
    return targetTable(options).isPresent();
  }

  /**
   * Names the warehouse table that a Fabric connector write loads. When the {@code DatawarehouseId}
   * option is set, the table's OneLake file location is added as a {@code LOCATION} symlink.
   *
   * @param options the options of the connector's first step
   * @param conf looks up a Spark setting
   * @return the table, or empty if these aren't connector options or the address can't be worked
   *     out
   */
  public static Optional<DatasetIdentifier> getWriteIdentifier(
      Map<String, String> options, Function<String, Optional<String>> conf) {
    Optional<String[]> table = targetTable(options);
    if (!table.isPresent()) {
      return Optional.empty();
    }
    String warehouse = table.get()[0];
    Optional<String> workspaceOption = option(options, FabricWarehouseUtils.WORKSPACE_ID_OPTION);

    String overrideKey = String.format(Locale.ROOT, ENDPOINT_OVERRIDE_CONF, warehouse);
    Optional<String> override = conf.apply(overrideKey).filter(value -> !value.trim().isEmpty());
    if (override.isPresent() && !FabricWarehouseUtils.isFabricEndpoint(override.get())) {
      // Reads only name an address as a Fabric one when it passes this check, so it can't be, for
      // example, an address for a workspace in another organization. Using the setting anyway
      // would give a table one name when written and another when read, and building the address
      // from IDs instead could name the wrong workspace. So the write is reported without output.
      // Logged at info because the user's own setting is being ignored.
      log.info(
          "The {} setting is not a Fabric workspace SQL address; emitting no output dataset",
          overrideKey);
      return Optional.empty();
    }
    Optional<String> endpoint =
        override.isPresent()
            ? override.map(FabricWarehouseUtils::canonicalEndpoint)
            : buildEndpoint(workspaceOption, conf);
    if (!endpoint.isPresent()) {
      log.debug(
          "Cannot resolve the Fabric Warehouse endpoint of {}; emitting no output dataset",
          String.join(".", table.get()));
      return Optional.empty();
    }

    DatasetIdentifier identifier =
        new DatasetIdentifier(
            String.join(".", table.get()), FabricWarehouseUtils.SCHEME + "://" + endpoint.get());
    return Optional.of(
        FabricWarehouseUtils.withOneLakeLocation(
            identifier,
            // the DatawarehouseId option is for the warehouse being written to
            Optional.of(warehouse),
            workspaceOption,
            option(options, FabricWarehouseUtils.DATAWAREHOUSE_ID_OPTION)));
  }

  /** Looks up a Spark setting in the session's settings, then in the application's settings. */
  public static Function<String, Optional<String>> confOf(SparkSession session) {
    return key -> {
      Optional<String> value = ScalaConversionUtils.asJavaOptional(session.conf().getOption(key));
      if (value.isPresent()) {
        return value;
      }
      return ScalaConversionUtils.asJavaOptional(session.sparkContext().getConf().getOption(key));
    };
  }

  private static Optional<String> buildEndpoint(
      Optional<String> workspaceOption, Function<String, Optional<String>> conf) {
    Optional<UUID> tenant = conf.apply(TENANT_ID_CONF).flatMap(FabricWarehouseUtils::parseGuid);
    Optional<UUID> workspace =
        workspaceOption.isPresent()
            ? workspaceOption.flatMap(FabricWarehouseUtils::parseGuid)
            : conf.apply(WORKSPACE_ID_CONF).flatMap(FabricWarehouseUtils::parseGuid);
    if (!tenant.isPresent() || !workspace.isPresent()) {
      return Optional.empty();
    }
    return Optional.of(FabricWarehouseUtils.endpointFor(tenant.get(), workspace.get()));
  }

  private static Optional<String[]> targetTable(Map<String, String> options) {
    return option(options, FabricWarehouseUtils.TABLE_OPTION)
        .map(value -> value.split("\\."))
        .filter(parts -> parts.length == TABLE_NAME_PARTS);
  }

  // option names may come in any mix of upper and lower case
  private static Optional<String> option(Map<String, String> options, String key) {
    return options.entrySet().stream()
        .filter(e -> key.equalsIgnoreCase(e.getKey()))
        .map(Map.Entry::getValue)
        .filter(value -> !value.trim().isEmpty())
        .findFirst();
  }
}
