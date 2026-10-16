/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.sql;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.SystemUtils;

/**
 * Main class for OpenLineage SQL parsing and lineage extraction. Provides static methods to parse
 * SQL statements and extract metadata.
 */
public final class OpenLineageSql {

  private static native SqlMeta parseNative(
      List<String> sql, String dialect, String defaultSchema, String defaultDatabase)
      throws RuntimeException;

  /**
   * Parses SQL statements with a specific dialect.
   *
   * @param sql list of SQL statements to parse
   * @param dialect the SQL dialect to use for parsing
   * @return optional SqlMeta containing parsed metadata, empty if parsing fails
   */
  public static Optional<SqlMeta> parse(List<String> sql, String dialect) {
    return parse(sql, dialect, null, null);
  }

  /**
   * Parses SQL statements with defaults for unqualified table names.
   *
   * @param sql list of SQL statements to parse
   * @param dialect the SQL dialect to use for parsing, or null for automatic detection
   * @param defaultSchema schema for tables without an explicit schema, or null
   * @param defaultDatabase database for tables without an explicit database, or null
   * @return optional SqlMeta containing parsed metadata, empty if parsing fails
   */
  public static Optional<SqlMeta> parse(
      List<String> sql, String dialect, String defaultSchema, String defaultDatabase) {
    if (loadError.isPresent()) {
      // TODO: pass error
      return Optional.empty();
    }
    try {
      return Optional.of(parseNative(sql, dialect, defaultSchema, defaultDatabase));
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  /**
   * Parses SQL statements using automatic dialect detection.
   *
   * @param sql list of SQL statements to parse
   * @return optional SqlMeta containing parsed metadata, empty if parsing fails
   */
  public static Optional<SqlMeta> parse(List<String> sql) {
    return parse(sql, null);
  }

  /**
   * Returns the provider information for the native library.
   *
   * @return provider string identifying the native library version
   */
  public static native String provider();

  /**
   * Contains any error that occurred during native library loading. Empty if the library loaded
   * successfully.
   */
  public static Optional<String> loadError = Optional.empty();

  private static void loadNativeLibrary(String libName) throws IOException {
    String fullName = "io/openlineage/sql/" + libName;

    URL url = OpenLineageSql.class.getResource("/" + fullName);
    if (url == null) {
      throw new IOException("Library not found in resources.");
    }

    File tmpDir = Files.createTempDirectory("native-lib").toFile();
    tmpDir.deleteOnExit();
    File nativeLib = new File(tmpDir, libName);
    nativeLib.deleteOnExit();

    try (InputStream in = url.openStream()) {
      Files.copy(in, nativeLib.toPath());
    }

    System.load(nativeLib.getAbsolutePath());
  }

  static {
    String libName = "libopenlineage_sql_java";
    if (SystemUtils.IS_OS_MAC_OSX && SystemUtils.OS_ARCH.equals("aarch64")) {
      libName += "_arm64.dylib";
    } else if (SystemUtils.IS_OS_MAC_OSX) {
      libName += ".dylib";
    } else if (SystemUtils.IS_OS_LINUX && SystemUtils.OS_ARCH.equals("aarch64")) {
      libName += "_aarch64.so";
    } else if (SystemUtils.IS_OS_LINUX && SystemUtils.OS_ARCH.equals("amd64")) {
      libName += "_x86_64.so";
    } else {
      loadError = Optional.of("Cannot link native library: unsupported OS");
    }

    try {
      loadNativeLibrary(libName);
    } catch (IOException e) {
      loadError =
          Optional.of(
              String.format("Error extracting native library '%s': %s", libName, e.getMessage()));
    }

    if (loadError.isPresent()) {
      System.err.println(loadError.get());
    }
  }
}
