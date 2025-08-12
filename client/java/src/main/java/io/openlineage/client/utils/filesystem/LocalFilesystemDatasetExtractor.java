/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.dataset.Naming;
import java.net.URI;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

public class LocalFilesystemDatasetExtractor implements FilesystemDatasetExtractor {
  private static final String SCHEME = "file";

  @Override
  public boolean isDefinedAt(URI location) {
    return location.getScheme() == null || location.getScheme().equals(SCHEME);
  }

  @Override
  public Naming.DatasetNaming extract(URI location) {
    String name =
        Optional.ofNullable(location.getPath())
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();
    return new Naming.LocalFileSystem(name);
  }

  @Override
  public Naming.DatasetNaming extractWarehouseDatasetNaming(URI location, String rawName) {
    String path = location.getPath();

    // For Windows paths without scheme (C:/home/test)
    if (isWindowsPath(location, path)) {
      String warehouseLocation = "file:" + path;
      return new Naming.WarehouseNaming(warehouseLocation, rawName);
    }

    // For file:// with authority (remote file)
    if (isRemoteFile(location)) {
      return new Naming.WarehouseNaming(location.toString(), rawName);
    }

    // For file: scheme
    if ("file".equals(location.getScheme())) {
      String warehouseLocation = FilesystemUriSanitizer.removeLastSlash(location.toString());
      // Normalize file:/// to file:/
      if (warehouseLocation.startsWith("file:///")) {
        warehouseLocation = "file:/" + warehouseLocation.substring(8);
      }
      return new Naming.WarehouseNaming(warehouseLocation, rawName);
    }

    // For paths without scheme
    if (StringUtils.isNotEmpty(path) && !"/".equals(path)) {
      String warehouseLocation = "file:" + path;
      return new Naming.WarehouseNaming(warehouseLocation, rawName);
    } else {
      return new Naming.WarehouseNaming("file", rawName);
    }
  }

  private boolean isWindowsPath(URI location, String path) {
    return location.getScheme() == null
        && path != null
        && (path.matches("^[A-Za-z]:/.*") || path.contains(":"));
  }

  private boolean isRemoteFile(URI location) {
    return "file".equals(location.getScheme()) && location.getAuthority() != null;
  }
}
