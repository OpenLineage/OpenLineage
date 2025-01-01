/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
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
  public DatasetIdentifier extract(URI location) {
    String name =
        Optional.ofNullable(location.getPath())
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();
    return new DatasetIdentifier(name, SCHEME);
  }

  @Override
  public DatasetIdentifier extract(URI location, String rawName) {
    String path = FilesystemUriSanitizer.removeLastSlash(location.getPath());

    String namespace;
    if (StringUtils.isNotEmpty(path)) {
      // `file:/path`
      namespace = String.format("%s:%s", SCHEME, path);
    } else {
      // `file`
      namespace = SCHEME;
    }

    String name =
        Optional.of(rawName)
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();
    return new DatasetIdentifier(name, namespace);
  }
}
