/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.gvfs.GVFSUtils;
import io.openlineage.client.utils.gravitino.GravitinoInfo;
import io.openlineage.client.utils.gravitino.GravitinoInfoManager;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;

public class GVFSFilesystemDatasetExtractor implements FilesystemDatasetExtractor {
  public static final String SCHEME = "gvfs";

  private final GravitinoInfoManager gravitinoInfoManager = GravitinoInfoManager.getInstance();

  @Override
  public boolean isDefinedAt(URI location) {
    return SCHEME.equalsIgnoreCase(location.getScheme());
  }

  @Override
  @SneakyThrows
  public DatasetIdentifier extract(URI location) {
    String namespace =
        getGravitinoNamespace()
            .orElse(
                new URI(location.getScheme(), location.getAuthority(), null, null, null)
                    .toString());
    String name = location.getPath();
    return new DatasetIdentifier(
        FilesystemUriSanitizer.nonEmptyPath(name),
        FilesystemUriSanitizer.removeLastSlash(namespace));
  }

  @Override
  @SneakyThrows
  public DatasetIdentifier extract(URI location, String rawName) {
    URI fixedLocation = new URI(SCHEME, location.getAuthority(), location.getPath(), null, null);
    String namespace =
        getGravitinoNamespace()
            .orElse(FilesystemUriSanitizer.removeLastSlash(fixedLocation.toString()));
    String name =
        Optional.of(rawName)
            .map(FilesystemUriSanitizer::removeFirstSlash)
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();
    return new DatasetIdentifier(name, namespace);
  }

  /**
   * Gets the Gravitino namespace if available.
   *
   * @return Optional Gravitino namespace, or empty if unavailable
   */
  private Optional<String> getGravitinoNamespace() {
    try {
      GravitinoInfo gravitinoInfo = gravitinoInfoManager.getGravitinoInfo();
      return GVFSUtils.getGravitinoNamespace(gravitinoInfo);
    } catch (Exception e) {
      // Fallback gracefully if Gravitino info is not available
      return Optional.empty();
    }
  }
}
