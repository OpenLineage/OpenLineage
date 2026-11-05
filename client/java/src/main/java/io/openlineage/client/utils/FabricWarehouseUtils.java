/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * How OpenLineage names tables in Microsoft Fabric warehouses.
 *
 * <p>Each Fabric workspace has one SQL address that all its warehouses share, for example {@code
 * 6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com}. A
 * table is named {@code fabric-warehouse://<address>} plus {@code <warehouse>.<schema>.<table>}.
 *
 * <p>Some of what this class relies on isn't documented by Microsoft: how the address is built from
 * the tenant and workspace IDs, and the option names that Microsoft's Spark connector for Fabric
 * uses. All of it is kept in this class, and the tests check it against addresses taken from real
 * workspaces, so a change on Microsoft's side only needs fixing here.
 */
public final class FabricWarehouseUtils {
  public static final String SCHEME = "fabric-warehouse";
  public static final String ENDPOINT_SUFFIX = ".datawarehouse.fabric.microsoft.com";

  /** Connector option holding the table being written, as {@code <warehouse>.<schema>.<table>}. */
  public static final String TABLE_OPTION = "three-part_sql_table_or_view_name";

  public static final String WORKSPACE_ID_OPTION = "WorkspaceId";
  public static final String DATAWAREHOUSE_ID_OPTION = "DatawarehouseId";

  /** Connector option naming the warehouse that a read's query runs in. */
  public static final String DATABASE_NAME_OPTION = "database_name";

  private static final String DEFAULT_PORT = "1433";
  private static final String ONELAKE_HOST = "onelake.dfs.fabric.microsoft.com";
  private static final String BASE32_ALPHABET = "abcdefghijklmnopqrstuvwxyz234567";
  private static final int GUID_BYTES = 16;
  private static final int GUID_BASE32_LENGTH = 26;
  private static final int TABLE_NAME_PARTS = 3;

  // "host", "host:port" or "host,port"; users set the connector's address setting as "host,port"
  private static final Pattern HOST_AND_PORT = Pattern.compile("^([^:,]+)(?:[:,](\\d+))?$");
  private static final Pattern ZONE_LABEL =
      Pattern.compile("\\.z[0-9a-f]{2}" + Pattern.quote(ENDPOINT_SUFFIX) + "$");
  private static final Pattern ENDPOINT_HOST =
      Pattern.compile(
          "^([a-z2-7]{"
              + GUID_BASE32_LENGTH
              + "})-([a-z2-7]{"
              + GUID_BASE32_LENGTH
              + "})"
              + Pattern.quote(ENDPOINT_SUFFIX)
              + "$");

  private FabricWarehouseUtils() {}

  /**
   * Normalizes a Fabric address, so the same table always gets the same name.
   *
   * <p>The address is lowercased, the {@code .z<two characters>} label that Fabric sometimes adds
   * before {@code .datawarehouse.fabric.microsoft.com} is removed, and port 1433, the default, is
   * removed. Any other port is kept as {@code :port}.
   *
   * @param endpoint {@code host}, {@code host:port} or {@code host,port}
   * @return the normalized address, or just lowercased if it can't be read
   */
  public static String canonicalEndpoint(String endpoint) {
    String value = endpoint.trim().toLowerCase(Locale.ROOT);
    Matcher matcher = HOST_AND_PORT.matcher(value);
    if (!matcher.matches()) {
      return value;
    }
    String host = ZONE_LABEL.matcher(matcher.group(1)).replaceFirst(ENDPOINT_SUFFIX);
    String port = matcher.group(2);
    if (port == null || DEFAULT_PORT.equals(port)) {
      return host;
    }
    return host + ":" + port;
  }

  /**
   * Whether this is the SQL address of a Fabric workspace, optionally followed by a numeric port.
   * Addresses used to reach a workspace in another organization have an extra part and aren't
   * matched.
   */
  public static boolean isFabricEndpoint(String endpoint) {
    String value = endpoint.trim().toLowerCase(Locale.ROOT);
    // anything after the host that isn't a numeric port, such as "host:bad", isn't an address
    if (!HOST_AND_PORT.matcher(value).matches()) {
      return false;
    }
    return ENDPOINT_HOST.matcher(hostOf(canonicalEndpoint(value))).matches();
  }

  /**
   * Builds a workspace's SQL address from its tenant ID and workspace ID. This is needed when only
   * the IDs are known, as with writes from Microsoft's Spark connector for Fabric.
   *
   * <p>The address is {@code <tenant part>-<workspace part>.datawarehouse.fabric.microsoft.com}.
   * Each part is the ID turned into 26 characters:
   *
   * <ol>
   *   <li>take the ID's 16 bytes in the order Microsoft's .NET stores them, which reverses the
   *       bytes within each of the first three groups of the ID;
   *   <li>write those bytes in base32 (letters a-z and digits 2-7), lowercase, without trailing
   *       {@code =} padding.
   * </ol>
   *
   * <p>For example, tenant {@code cc0b82f3-7c2e-400b-aec3-40a3d720505b} and workspace {@code
   * 8b4cb800-a044-43ce-8f76-3356f709b468} give {@code
   * 6obaxtbopqfublwdicr5oicqlm-ac4ezc2eudhehd3wgnlpocnuna.datawarehouse.fabric.microsoft.com}.
   *
   * <p>Microsoft doesn't document this. In November 2026 it matched the real addresses of 2 test
   * workspaces and 44 public tenants, and Microsoft's own {@code fabric-warehouse-labs} samples
   * build the address the same way. If Microsoft changes it, update this method and the known
   * examples in {@code FabricWarehouseUtilsTest}. Users can also set the address themselves; see
   * the Spark integration's Microsoft Fabric docs.
   */
  public static String endpointFor(UUID tenantId, UUID workspaceId) {
    return encodeGuid(tenantId) + "-" + encodeGuid(workspaceId) + ENDPOINT_SUFFIX;
  }

  /** Reads the workspace ID back out of a Fabric SQL address. */
  public static Optional<UUID> workspaceIdOf(String endpoint) {
    Matcher matcher = ENDPOINT_HOST.matcher(hostOf(canonicalEndpoint(endpoint)));
    if (!matcher.matches()) {
      return Optional.empty();
    }
    return decodeGuid(matcher.group(2));
  }

  /**
   * Builds the location of a table's files in OneLake, Fabric's storage. The path always uses IDs,
   * never names, because OneLake refuses paths that mix the two (error 400 {@code
   * FriendlyNameSupportDisabled}).
   *
   * @return namespace {@code abfss://<workspace ID>@onelake.dfs.fabric.microsoft.com} and name
   *     {@code <warehouse ID>/Tables/<schema>/<table>}
   */
  public static DatasetIdentifier oneLakeLocation(
      UUID workspaceId, UUID warehouseId, String schema, String table) {
    return new DatasetIdentifier(
        warehouseId + "/Tables/" + schema + "/" + table,
        "abfss://" + workspaceId + "@" + ONELAKE_HOST);
  }

  /**
   * Adds the table's OneLake file location to a Fabric warehouse dataset, as a {@code LOCATION}
   * symlink.
   *
   * <p>This needs the warehouse's ID, which is only available when the user passes it with the
   * connector's {@code DatawarehouseId} option. That ID belongs to one warehouse, but a query can
   * read tables from several warehouses in the same workspace. So the location is only added to
   * tables in the warehouse the ID was given for; other tables would get a wrong path.
   *
   * <p>If anything is missing, the table is in another warehouse, or an ID isn't valid, the dataset
   * is returned as it was.
   *
   * @param identifier a {@code fabric-warehouse} dataset named {@code <warehouse>.<schema>.<table>}
   * @param idWarehouse name of the warehouse that {@code warehouseIdOption} belongs to
   * @param workspaceIdOption the {@code WorkspaceId} option; if not set, the workspace ID is read
   *     from the address
   * @param warehouseIdOption the {@code DatawarehouseId} option
   * @return the same dataset, with the location added when possible
   */
  public static DatasetIdentifier withOneLakeLocation(
      DatasetIdentifier identifier,
      Optional<String> idWarehouse,
      Optional<String> workspaceIdOption,
      Optional<String> warehouseIdOption) {
    String prefix = SCHEME + "://";
    String[] parts = identifier.getName().split("\\.");
    if (!identifier.getNamespace().startsWith(prefix)
        || parts.length != TABLE_NAME_PARTS
        || !idWarehouse.filter(parts[0]::equalsIgnoreCase).isPresent()) {
      return identifier;
    }
    Optional<UUID> warehouseId = warehouseIdOption.flatMap(FabricWarehouseUtils::parseGuid);
    Optional<UUID> workspaceId =
        workspaceIdOption.isPresent()
            ? workspaceIdOption.flatMap(FabricWarehouseUtils::parseGuid)
            : workspaceIdOf(identifier.getNamespace().substring(prefix.length()));
    if (!warehouseId.isPresent() || !workspaceId.isPresent()) {
      return identifier;
    }
    DatasetIdentifier location =
        oneLakeLocation(workspaceId.get(), warehouseId.get(), parts[1], parts[2]);
    return identifier.withSymlink(
        location.getName(), location.getNamespace(), DatasetIdentifier.SymlinkType.LOCATION);
  }

  /**
   * Describes a Fabric warehouse table in OneLake terms, for the {@code oneLake} facet.
   *
   * <p>Everything comes from the dataset itself, never from the Spark session, because a table can
   * be in another workspace than the one the job runs in:
   *
   * <ul>
   *   <li>the item type is {@code Warehouse} and the item name is the warehouse;
   *   <li>the workspace ID is read from the SQL address;
   *   <li>the warehouse ID and the table's location come from the {@code LOCATION} symlink that
   *       {@link #withOneLakeLocation} adds, and only if that symlink is in the same workspace as
   *       the address.
   * </ul>
   *
   * @param identifier a dataset, possibly with symlinks
   * @return the description, or empty if this isn't a {@code fabric-warehouse} dataset named {@code
   *     <warehouse>.<schema>.<table>}
   */
  public static Optional<OneLakeLocation> toOneLakeLocation(DatasetIdentifier identifier) {
    String prefix = SCHEME + "://";
    String[] parts = identifier.getName().split("\\.");
    if (!identifier.getNamespace().startsWith(prefix) || parts.length != TABLE_NAME_PARTS) {
      return Optional.empty();
    }
    Optional<UUID> workspaceId =
        workspaceIdOf(identifier.getNamespace().substring(prefix.length()));
    OneLakeLocation.OneLakeLocationBuilder builder =
        OneLakeLocation.builder()
            .workspaceId(workspaceId.orElse(null))
            .itemName(parts[0])
            .itemType(OneLakeLocation.WAREHOUSE)
            .path(Arrays.asList(OneLakeLocation.TABLES, parts[1], parts[2]));
    if (identifier.getSymlinks() != null && workspaceId.isPresent()) {
      identifier.getSymlinks().stream()
          .filter(s -> s.getType() == DatasetIdentifier.SymlinkType.LOCATION)
          .map(s -> OneLakeLocation.parse(s.getNamespace(), s.getName()))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .filter(location -> workspaceId.get().equals(location.getWorkspaceId()))
          .filter(location -> location.getItemId() != null)
          .findFirst()
          .ifPresent(location -> builder.itemId(location.getItemId()).path(location.getPath()));
    }
    return Optional.of(builder.build());
  }

  /** Reads an ID, returning empty instead of failing when the text isn't a valid ID. */
  public static Optional<UUID> parseGuid(String value) {
    if (value == null) {
      return Optional.empty();
    }
    try {
      return Optional.of(UUID.fromString(value.trim()));
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
  }

  static String encodeGuid(UUID guid) {
    byte[] bytes = toDotNetBytes(guid);
    StringBuilder result = new StringBuilder(GUID_BASE32_LENGTH);
    int buffer = 0;
    int bits = 0;
    for (byte b : bytes) {
      buffer = (buffer << 8) | (b & 0xff);
      bits += 8;
      while (bits >= 5) {
        result.append(BASE32_ALPHABET.charAt((buffer >> (bits - 5)) & 0x1f));
        bits -= 5;
      }
    }
    if (bits > 0) {
      result.append(BASE32_ALPHABET.charAt((buffer << (5 - bits)) & 0x1f));
    }
    return result.toString();
  }

  static Optional<UUID> decodeGuid(String label) {
    if (label.length() != GUID_BASE32_LENGTH) {
      return Optional.empty();
    }
    byte[] bytes = new byte[GUID_BYTES];
    int buffer = 0;
    int bits = 0;
    int index = 0;
    for (char c : label.toCharArray()) {
      int value = BASE32_ALPHABET.indexOf(c);
      if (value < 0) {
        return Optional.empty();
      }
      buffer = (buffer << 5) | value;
      bits += 5;
      if (bits >= Byte.SIZE) {
        bytes[index++] = (byte) (buffer >> (bits - Byte.SIZE));
        bits -= Byte.SIZE;
      }
    }
    // 26 characters hold 130 bits, 2 more than the ID's 128; those 2 must be zero
    if ((buffer & ((1 << bits) - 1)) != 0) {
      return Optional.empty();
    }
    return Optional.of(fromDotNetBytes(bytes));
  }

  // The byte order of .NET's Guid.ToByteArray(): the bytes of each of the first three groups of
  // the ID are reversed; the last 8 bytes stay as written
  private static byte[] toDotNetBytes(UUID guid) {
    ByteBuffer bigEndian = ByteBuffer.allocate(GUID_BYTES);
    bigEndian.putLong(guid.getMostSignificantBits()).putLong(guid.getLeastSignificantBits());
    byte[] bytes = bigEndian.array();
    ByteBuffer result = ByteBuffer.allocate(GUID_BYTES).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer source = ByteBuffer.wrap(bytes);
    result.putInt(source.getInt()).putShort(source.getShort()).putShort(source.getShort());
    result.put(bytes, 8, 8);
    return result.array();
  }

  private static UUID fromDotNetBytes(byte[] bytes) {
    ByteBuffer source = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    ByteBuffer bigEndian = ByteBuffer.allocate(GUID_BYTES);
    bigEndian.putInt(source.getInt()).putShort(source.getShort()).putShort(source.getShort());
    bigEndian.put(bytes, 8, 8);
    // read by position: ByteBuffer.flip() doesn't exist on Java 8 when compiled with a newer JDK
    return new UUID(bigEndian.getLong(0), bigEndian.getLong(Long.BYTES));
  }

  private static String hostOf(String canonicalEndpoint) {
    int colon = canonicalEndpoint.indexOf(':');
    return colon < 0 ? canonicalEndpoint : canonicalEndpoint.substring(0, colon);
  }
}
