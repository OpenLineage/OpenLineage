/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;

public class ObjectStorageDatasetExtractor implements FilesystemDatasetExtractor {
  private final String scheme;

  public ObjectStorageDatasetExtractor(String scheme) {
    this.scheme = scheme;
  }

  @Override
  public boolean isDefinedAt(URI location) {
    return location.getScheme().startsWith(scheme);
  }

  @Override
  @SneakyThrows
  public DatasetIdentifier extract(URI location) {
    URI namespace = new URI(location.getScheme(), location.getAuthority(), null, null, null);
    String name = location.getPath();
    return extract(namespace, name);
  }

  @Override
  @SneakyThrows
  public DatasetIdentifier extract(URI location, String rawName) {
    URI fixedLocation = new URI(scheme, location.getAuthority(), location.getPath(), null, null);
    String namespace = FilesystemUriSanitizer.removeLastSlash(fixedLocation.toString());
    String name =
        Optional.of(rawName)
            .map(FilesystemUriSanitizer::removeFirstSlash)
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();
    return new DatasetIdentifier(name, namespace);
  }
}
