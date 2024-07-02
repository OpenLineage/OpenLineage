/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import lombok.SneakyThrows;

public class FilesystemDatasetUtils {
  private static final FilesystemDatasetExtractor[] extractors = {
    new LocalFilesystemDatasetExtractor(),
    new ObjectStorageDatasetExtractor("s3"),
    new ObjectStorageDatasetExtractor("gs"),
    new ObjectStorageDatasetExtractor("wasbs")
  };

  private static FilesystemDatasetExtractor getExtractor(URI location) {
    for (FilesystemDatasetExtractor extractor : extractors) {
      if (extractor.isDefinedAt(location)) {
        return extractor;
      }
    }
    return new GenericFilesystemDatasetExtractor();
  }

  /**
   * Get DatasetIdentifier from table location URI
   *
   * @param location filesystem location
   * @return DatasetIdentifier
   */
  public static DatasetIdentifier fromLocation(URI location) {
    FilesystemDatasetExtractor extractor = getExtractor(location);
    return extractor.extract(location);
  }

  /**
   * Get DatasetIdentifier from table location URI and name
   *
   * @param location filesystem location
   * @param name table name
   * @return DatasetIdentifier
   */
  public static DatasetIdentifier fromLocationAndName(URI location, String name) {
    FilesystemDatasetExtractor extractor = getExtractor(location);
    return extractor.extract(location, name);
  }

  /**
   * Get URI from DatasetIdentifier
   *
   * @param di dataset identifier
   * @return URI
   */
  @SneakyThrows
  public static URI toLocation(DatasetIdentifier di) {
    String namespace = di.getNamespace();
    if (!namespace.contains(":/")) {
      // 'file'
      namespace = namespace + ":/";
    }
    String name = di.getName();
    if ("/".equals(name)) {
      return new URI(namespace);
    }

    String location =
        FilesystemUriSanitizer.removeLastSlash(namespace)
            + "/"
            + FilesystemUriSanitizer.removeFirstSlash(name);
    return new URI(location);
  }
}
