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
  public Naming.DatasetNaming extract(URI location, String rawName) {
    String path = location.getPath();
    String processedName =
        Optional.of(rawName)
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();

    if (isWindowsPath(location, path)) {
      String combinedName = FilesystemUriSanitizer.removeLastSlash(path) + "/" + processedName;
      return new Naming.LocalFileSystem(combinedName);
    }

    if (isRemoteFile(location)) {
      return handleRemoteFileSystem(location, path, processedName);
    }

    if ("file".equals(location.getScheme())) {
      return handleFileScheme(location, path, processedName);
    }

    return handleNoScheme(path, processedName);
  }

  private boolean isWindowsPath(URI location, String path) {
    return location.getScheme() == null
        && path != null
        && (path.matches("^[A-Za-z]:/.*") || path.contains(":"));
  }

  private boolean isRemoteFile(URI location) {
    return "file".equals(location.getScheme()) && location.getAuthority() != null;
  }

  private Naming.DatasetNaming handleRemoteFileSystem(
      URI location, String path, String processedName) {
    String host;
    String remainingPath;

    if (location.getAuthority() != null) {
      host = location.getAuthority();
      remainingPath = path != null && path.startsWith("/") ? path.substring(1) : path;
    } else {
      String[] pathParts = path.substring(1).split("/", 2);
      host = pathParts[0];
      remainingPath = pathParts.length > 1 ? pathParts[1] : ""; // location
    }

    String combinedName =
        (remainingPath == null || remainingPath.isEmpty())
            ? processedName
            : remainingPath + "/" + processedName;
    return new Naming.RemoteFileSystem(host, combinedName);
  }

  private Naming.DatasetNaming handleFileScheme(URI location, String path, String processedName) {
    String slash = "/";
    if (StringUtils.isEmpty(path) || slash.equals(path)) {
      return new Naming.LocalFileSystem(processedName);
    }

    if (location.toString().contains("file:///")) {
      String pathName = FilesystemUriSanitizer.removeLastSlash(path);
      if (pathName.startsWith(slash)) {
        pathName = pathName.substring(1); // Remove leading slash
      }
      return new Naming.LocalFileSystem(pathName + slash + processedName);
    }

    if (path.endsWith(slash)) {
      String pathName = FilesystemUriSanitizer.removeLastSlash(path);
      return new Naming.LocalFileSystem(pathName + slash + processedName);
    } else {
      return new Naming.LocalFileSystem(processedName);
    }
  }

  private Naming.DatasetNaming handleNoScheme(String path, String processedName) {
    if (StringUtils.isNotEmpty(path) && !"/".equals(path)) {
      String pathName = FilesystemUriSanitizer.removeLastSlash(path);
      return new Naming.LocalFileSystem(pathName + "/" + processedName);
    } else {
      return new Naming.LocalFileSystem(processedName);
    }
  }
}
