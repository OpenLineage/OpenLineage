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
    // Now that GVFSFilesystemDatasetExtractor extends GenericFilesystemDatasetExtractor,
    // the extract(URI location) method uses the parent's behavior:
    // - namespace = scheme + authority (e.g., "gvfs://fileset")
    // - name = path (e.g., "/catalog_name/schema_name/fileset_name/location")

    DatasetIdentifier di =
        FilesystemDatasetUtils.fromLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"));
    Assertions.assertEquals("gvfs://fileset", di.getNamespace());
    Assertions.assertEquals("/catalog_name/schema_name/fileset_name/location", di.getName());

    // Verify symlinks structure exists (even if empty in unit tests without Gravitino config)
    Assertions.assertNotNull(di.getSymlinks());
    // Note: In a real environment with Gravitino configuration, symlinks would contain:
    // - name: "catalog_name.schema_name.fileset_name"
    // - namespace: "{gravitino_uri}/api/metalakes/{metalake}"
    // - type: LOCATION
    // In unit tests without ServiceLoader providers, the symlinks list will be empty

    di =
        FilesystemDatasetUtils.fromLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));
    Assertions.assertEquals("gvfs://fileset", di.getNamespace());
    Assertions.assertEquals("/catalog_name/schema_name/fileset_name", di.getName());
    Assertions.assertNotNull(di.getSymlinks());

    // The parent method doesn't validate GVFS path format, so this won't throw an exception
    di = FilesystemDatasetUtils.fromLocation(new URI("gvfs://fileset/catalog_name"));
    Assertions.assertEquals("gvfs://fileset", di.getNamespace());
    Assertions.assertEquals("/catalog_name", di.getName());
    Assertions.assertNotNull(di.getSymlinks());
  }

  @Test
  @SneakyThrows
  void testFromLocationAndName() {
    // Test the extract(URI location, String rawName) method which adds symlinks
    DatasetIdentifier di =
        FilesystemDatasetUtils.fromLocationAndName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"),
            "catalog_name.schema_name.fileset_name");

    // The parent method behavior: namespace from full URI string, name from rawName parameter
    Assertions.assertEquals(
        "gvfs://fileset/catalog_name/schema_name/fileset_name/location", di.getNamespace());
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", di.getName());

    // Verify symlinks structure exists (even if empty in unit tests without Gravitino config)
    Assertions.assertNotNull(di.getSymlinks());
    // Note: In a real environment with Gravitino configuration, symlinks would contain:
    // - name: "catalog_name.schema_name.fileset_name"
    // - namespace: "{gravitino_uri}/api/metalakes/{metalake}"
    // - type: LOCATION
    // In unit tests without ServiceLoader providers, the symlinks list will be empty
  }
}
