/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.filesystem.gvfs.GVFSUtils;
import io.openlineage.client.utils.gravitino.GravitinoInfoProviderImpl;
import java.net.URI;

public class GVFSFilesystemDatasetExtractor implements FilesystemDatasetExtractor {
  public static final String SCHEME = "gvfs";
  public static final String GVFS_NAMESPACE_NAME = "__GVFS_NAMESPACE";

  private final GravitinoInfoProviderImpl metalakeProvider =
      GravitinoInfoProviderImpl.getInstance();

  @Override
  public boolean isDefinedAt(URI location) {
    return SCHEME.equalsIgnoreCase(location.getScheme());
  }

  @Override
  public DatasetIdentifier extract(URI location) {
    String name = GVFSUtils.getGVFSIdentifierName(location);
    String namespace;
    try {
      namespace = metalakeProvider.getMetalakeName();
    } catch (IllegalStateException e) {
      // Fallback to default namespace when Gravitino is not available (e.g., in tests)
      namespace = GVFS_NAMESPACE_NAME;
    }
    return new DatasetIdentifier(name, namespace);
  }

  @Override
  public DatasetIdentifier extract(URI location, String rawName) {
    return extract(location);
  }
}
