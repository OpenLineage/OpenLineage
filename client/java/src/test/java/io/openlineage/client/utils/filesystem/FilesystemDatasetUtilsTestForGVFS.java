/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class FilesystemDatasetUtilsTestForGVFS {

  @Test
  @SneakyThrows
  void testFromLocation() {
    String namespaceName = GVFSFilesystemDatasetExtractor.GVFS_NAMESPACE_NAME;

    DatasetIdentifier di =
        FilesystemDatasetUtils.fromLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"));
    Assertions.assertEquals(namespaceName, di.getNamespace());
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", di.getName());

    di =
        FilesystemDatasetUtils.fromLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));
    Assertions.assertEquals(namespaceName, di.getNamespace());
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", di.getName());

    Assertions.assertThrowsExactly(
        IllegalArgumentException.class,
        () -> FilesystemDatasetUtils.fromLocation(new URI("gvfs://fileset/catalog_name")));
  }
}
