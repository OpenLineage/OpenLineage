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

public class GVFSFilesystemDatasetExtractor extends GenericFilesystemDatasetExtractor {
  public static final String SCHEME = "gvfs";
  public static final String GVFS_NAMESPACE_NAME = "__GVFS_NAMESPACE";

  private final GravitinoInfoManager gravitinoInfoManager = GravitinoInfoManager.getInstance();

  @Override
  public boolean isDefinedAt(URI location) {
    return SCHEME.equalsIgnoreCase(location.getScheme());
  }

  @Override
  public DatasetIdentifier extract(URI location) {
    DatasetIdentifier parentResult = super.extract(location);
    return addGravitinoSymlink(parentResult, location);
  }

  @Override
  public DatasetIdentifier extract(URI location, String rawName) {
    DatasetIdentifier parentResult = super.extract(location, rawName);
    return addGravitinoSymlink(parentResult, location);
  }

  /**
   * Adds Gravitino-specific symlink information to a DatasetIdentifier.
   *
   * @param datasetIdentifier the dataset identifier to enhance
   * @param location the GVFS URI location
   * @return the enhanced dataset identifier with symlink (or original if Gravitino info
   *     unavailable)
   */
  private DatasetIdentifier addGravitinoSymlink(DatasetIdentifier datasetIdentifier, URI location) {
    try {
      GravitinoInfo gravitinoInfo = gravitinoInfoManager.getGravitinoInfo();
      Optional<String> gravitinoUri = gravitinoInfo.getUri();
      Optional<String> metalake = gravitinoInfo.getMetalake();

      if (gravitinoUri.isPresent() && metalake.isPresent()) {
        String symlinkNamespace = gravitinoUri.get() + "/api/metalakes/" + metalake.get();
        String symlinkName = GVFSUtils.getGVFSIdentifierName(location);

        datasetIdentifier.withSymlink(
            symlinkName, symlinkNamespace, DatasetIdentifier.SymlinkType.LOCATION);
      }
    } catch (Exception e) {
      // Fallback gracefully if Gravitino info is not available
      // The dataset identifier will be returned without the symlink
    }

    return datasetIdentifier;
  }
}
