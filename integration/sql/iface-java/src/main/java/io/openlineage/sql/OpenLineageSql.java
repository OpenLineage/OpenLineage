/*
/* Copyright 2018-2022 contributors to the OpenLineage project
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

public final class OpenLineageSql {

  // TODO: wrap defaultSchema
  private static native SqlMeta parse(List<String> sql, String dialect, String defaultSchema)
      throws RuntimeException;

  public static Optional<SqlMeta> parse(List<String> sql, String dialect) {
    if (loadError.isPresent()) {
      // TODO: pass error
      return Optional.empty();
    }
    try {
      return Optional.of(parse(sql, dialect, null));
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  public static Optional<SqlMeta> parse(List<String> sql) {
    if (loadError.isPresent()) {
      // TODO: pass error
      return Optional.empty();
    }
    try {
      return Optional.of(parse(sql, null, null));
    } catch (RuntimeException e) {
      return Optional.empty();
    }
  }

  public static native String provider();

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
    if (SystemUtils.IS_OS_MAC_OSX) {
      libName += ".dylib";
    } else if (SystemUtils.IS_OS_LINUX) {
      libName += ".so";
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
  }
}
