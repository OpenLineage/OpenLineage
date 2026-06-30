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
  private final String canonicalScheme;

  public ObjectStorageDatasetExtractor(String scheme) {
    this(scheme, scheme);
  }

  /**
   * @param scheme the scheme prefix this extractor matches (e.g. {@code abfs}, which also matches
   *     {@code abfss} since matching is by prefix)
   * @param canonicalScheme the scheme emitted in the resulting namespace; matched variants are
   *     normalized to it (e.g. {@code abfs} and {@code abfss} both normalize to {@code abfss})
   */
  public ObjectStorageDatasetExtractor(String scheme, String canonicalScheme) {
    this.scheme = scheme;
    this.canonicalScheme = canonicalScheme;
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
    URI fixedLocation =
        new URI(canonicalScheme, location.getAuthority(), location.getPath(), null, null);
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
