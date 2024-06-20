/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;

public class GenericFilesystemDatasetExtractor implements FilesystemDatasetExtractor {
  @Override
  public boolean isDefinedAt(URI location) {
    return true;
  }

  @Override
  @SneakyThrows
  public DatasetIdentifier extract(URI location) {
    URI namespace = new URI(location.getScheme(), location.getAuthority(), "/", null, null);
    String name = location.getPath();
    return extract(namespace, name);
  }

  @Override
  public DatasetIdentifier extract(URI location, String rawName) {
    String namespace = FilesystemUriSanitizer.removeLastSlash(location.toString());
    String name =
        Optional.of(rawName)
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();
    return new DatasetIdentifier(name, namespace);
  }
}
