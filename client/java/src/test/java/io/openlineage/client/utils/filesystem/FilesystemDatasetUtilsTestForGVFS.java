/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.gravitino.GravitinoInfo;
import io.openlineage.client.utils.gravitino.GravitinoInfoManager;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class FilesystemDatasetUtilsTestForGVFS {

  private GravitinoInfoManager mockManager;

  @BeforeEach
  void setUp() {
    mockManager = Mockito.mock(GravitinoInfoManager.class);
  }

  @AfterEach
  void tearDown() {
    // Reset any static mocks
    Mockito.reset(mockManager);
  }

  @Test
  @SneakyThrows
  void testFromLocationWithoutGravitinoConfig() {
    // Test the extract(URI location) method
    // Without Gravitino configuration, it falls back to GVFS scheme behavior:
    // - namespace = scheme + authority (e.g., "gvfs://fileset")
    // - name = path (e.g., "/catalog_name/schema_name/fileset_name/location")

    DatasetIdentifier di =
        FilesystemDatasetUtils.fromLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"));
    Assertions.assertEquals("gvfs://fileset", di.getNamespace());
    Assertions.assertEquals("/catalog_name/schema_name/fileset_name/location", di.getName());

    di =
        FilesystemDatasetUtils.fromLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));
    Assertions.assertEquals("gvfs://fileset", di.getNamespace());
    Assertions.assertEquals("/catalog_name/schema_name/fileset_name", di.getName());

    di = FilesystemDatasetUtils.fromLocation(new URI("gvfs://fileset/catalog_name"));
    Assertions.assertEquals("gvfs://fileset", di.getNamespace());
    Assertions.assertEquals("/catalog_name", di.getName());
  }

  @Test
  @SneakyThrows
  void testFromLocationWithGravitinoConfig() {
    // Test the extract(URI location) method with Gravitino configuration
    // With Gravitino configuration available:
    // - namespace = gravitino URI + "/api/metalakes/" + metalake
    // - name = path from URI

    // Mock Gravitino configuration
    GravitinoInfo mockInfo =
        GravitinoInfo.builder()
            .uri(Optional.of("https://gravitino-server:8090"))
            .metalake(Optional.of("test-metalake"))
            .build();

    // Create a test instance with mocked manager
    GVFSFilesystemDatasetExtractor testExtractor = createExtractorWithMockedManager(mockInfo);

    DatasetIdentifier di =
        testExtractor.extract(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"));

    Assertions.assertEquals(
        "https://gravitino-server:8090/api/metalakes/test-metalake", di.getNamespace());
    Assertions.assertEquals("/catalog_name/schema_name/fileset_name/location", di.getName());

    di = testExtractor.extract(new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));

    Assertions.assertEquals(
        "https://gravitino-server:8090/api/metalakes/test-metalake", di.getNamespace());
    Assertions.assertEquals("/catalog_name/schema_name/fileset_name", di.getName());
  }

  @Test
  @SneakyThrows
  void testFromLocationAndNameWithoutGravitinoConfig() {
    // Test the extract(URI location, String rawName) method
    // Without Gravitino configuration, it falls back to GVFS scheme behavior:
    // - namespace = full URI without query/fragment (e.g.,
    // "gvfs://fileset/catalog_name/schema_name/fileset_name/location")
    // - name = rawName parameter after sanitization

    DatasetIdentifier di =
        FilesystemDatasetUtils.fromLocationAndName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"),
            "catalog_name.schema_name.fileset_name");

    Assertions.assertEquals(
        "gvfs://fileset/catalog_name/schema_name/fileset_name/location", di.getNamespace());
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", di.getName());

    // Test with different rawName
    di =
        FilesystemDatasetUtils.fromLocationAndName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"), "my_table");

    Assertions.assertEquals(
        "gvfs://fileset/catalog_name/schema_name/fileset_name", di.getNamespace());
    Assertions.assertEquals("my_table", di.getName());
  }

  @Test
  @SneakyThrows
  void testFromLocationAndNameWithGravitinoConfig() {
    // Test the extract(URI location, String rawName) method with Gravitino configuration
    // With Gravitino configuration available:
    // - namespace = gravitino URI + "/api/metalakes/" + metalake
    // - name = rawName parameter after sanitization

    // Mock Gravitino configuration
    GravitinoInfo mockInfo =
        GravitinoInfo.builder()
            .uri(Optional.of("https://gravitino-server:8090"))
            .metalake(Optional.of("test-metalake"))
            .build();

    // Create a test instance with mocked manager
    GVFSFilesystemDatasetExtractor testExtractor = createExtractorWithMockedManager(mockInfo);

    DatasetIdentifier di =
        testExtractor.extract(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"),
            "catalog_name.schema_name.fileset_name");

    Assertions.assertEquals(
        "https://gravitino-server:8090/api/metalakes/test-metalake", di.getNamespace());
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", di.getName());

    // Test with different rawName
    di =
        testExtractor.extract(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"), "my_table");

    Assertions.assertEquals(
        "https://gravitino-server:8090/api/metalakes/test-metalake", di.getNamespace());
    Assertions.assertEquals("my_table", di.getName());
  }

  @Test
  @SneakyThrows
  void testFromLocationWithPartialGravitinoConfig() {
    // Test behavior when Gravitino configuration is incomplete
    // Should fall back to GVFS scheme behavior

    // Mock incomplete Gravitino configuration (missing metalake)
    GravitinoInfo mockInfo =
        GravitinoInfo.builder()
            .uri(Optional.of("https://gravitino-server:8090"))
            .metalake(Optional.empty())
            .build();

    GVFSFilesystemDatasetExtractor testExtractor = createExtractorWithMockedManager(mockInfo);

    DatasetIdentifier di =
        testExtractor.extract(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/location"));

    // Should fall back to GVFS scheme behavior
    Assertions.assertEquals("gvfs://fileset", di.getNamespace());
    Assertions.assertEquals("/catalog_name/schema_name/fileset_name/location", di.getName());
  }

  /**
   * Creates a GVFSFilesystemDatasetExtractor with a mocked GravitinoInfoManager using reflection to
   * replace the private field.
   */
  @SneakyThrows
  @SuppressWarnings("PMD.AvoidAccessibilityAlteration")
  private GVFSFilesystemDatasetExtractor createExtractorWithMockedManager(GravitinoInfo mockInfo) {
    GVFSFilesystemDatasetExtractor testExtractor = new GVFSFilesystemDatasetExtractor();
    GravitinoInfoManager mockManager = Mockito.mock(GravitinoInfoManager.class);
    Mockito.when(mockManager.getGravitinoInfo()).thenReturn(mockInfo);

    // Use reflection to replace the private gravitinoInfoManager field
    Field field = GVFSFilesystemDatasetExtractor.class.getDeclaredField("gravitinoInfoManager");
    field.setAccessible(true);
    field.set(testExtractor, mockManager);

    return testExtractor;
  }
}
