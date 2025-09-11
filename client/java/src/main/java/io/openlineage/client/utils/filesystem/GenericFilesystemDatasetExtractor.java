/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.dataset.Naming;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

public class GenericFilesystemDatasetExtractor implements FilesystemDatasetExtractor {
  @Override
  public boolean isDefinedAt(URI location) {
    return true;
  }

  @Override
  @SneakyThrows
  public Naming.DatasetNaming extract(URI location) {
    URI namespace = new URI(location.getScheme(), location.getAuthority(), "/", null, null);
    String name = location.getPath();
    return extractWarehouseDatasetNaming(namespace, name);
  }

  @Override
  public Naming.DatasetNaming extractWarehouseDatasetNaming(URI location, String rawName) {
    String namespace =
        Optional.of(location.toString())
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(scheme -> StringUtils.stripEnd(scheme, ":"))
            .get();

    String name =
        Optional.of(rawName)
            .map(FilesystemUriSanitizer::removeLastSlash)
            .map(FilesystemUriSanitizer::nonEmptyPath)
            .get();
    return new Naming.DatasetNaming() {
      @Override
      public String getNamespace() {
        return namespace;
      }

      @Override
      public String getName() {
        return name;
      }
    };
  }
}
