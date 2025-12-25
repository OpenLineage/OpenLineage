/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.openlineage.client.utils.filesystem.gvfs.GVFSUtils;
import java.net.URI;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class GVFSUtilsTest {

  @Test
  @SneakyThrows
  void testIsGVFS() {
    Assertions.assertTrue(
        GVFSUtils.isGVFS(new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/a/b/")));
    Assertions.assertFalse(
        GVFSUtils.isGVFS(new URI("gvfs1://fileset/catalog_name/schema_name/fileset_name/a/b/")));
    Assertions.assertFalse(
        GVFSUtils.isGVFS(new URI("gvf://fileset/catalog_name/schema_name/fileset_name/a/b/")));
    Assertions.assertFalse(
        GVFSUtils.isGVFS(new URI("s3://fileset/catalog_name/schema_name/fileset_name/a/b/")));
  }

  @Test
  @SneakyThrows
  void testIsGVFSCaseInsensitive() {
    // Test case insensitivity
    Assertions.assertTrue(
        GVFSUtils.isGVFS(new URI("GVFS://fileset/catalog_name/schema_name/fileset_name/")));
    Assertions.assertTrue(
        GVFSUtils.isGVFS(new URI("Gvfs://fileset/catalog_name/schema_name/fileset_name/")));
    Assertions.assertTrue(
        GVFSUtils.isGVFS(new URI("gVfS://fileset/catalog_name/schema_name/fileset_name/")));
  }

  @Test
  @SneakyThrows
  void testGetGVFSLocationSubpath() {
    String location =
        GVFSUtils.getGVFSLocationSubpath(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/a/b/c"));
    Assertions.assertEquals("/a/b/c", location);

    location =
        GVFSUtils.getGVFSLocationSubpath(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/a/b/c/"));
    Assertions.assertEquals("/a/b/c/", location);

    location =
        GVFSUtils.getGVFSLocationSubpath(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/"));
    Assertions.assertEquals("/", location);

    location =
        GVFSUtils.getGVFSLocationSubpath(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));
    Assertions.assertEquals("/", location);
  }

  @Test
  @SneakyThrows
  void testGetGVFSLocationSubpathWithDeepPath() {
    String location =
        GVFSUtils.getGVFSLocationSubpath(
            new URI(
                "gvfs://fileset/catalog/schema/fileset/year=2024/month=01/day=15/data.parquet"));
    Assertions.assertEquals("/year=2024/month=01/day=15/data.parquet", location);
  }

  @Test
  @SneakyThrows
  void testGetGVFSLocationWithSpecialCharacters() {
    String location =
        GVFSUtils.getGVFSLocationSubpath(
            new URI(
                "gvfs://fileset/catalog/schema/fileset/path_with_underscore/file-with-dash.txt"));
    Assertions.assertEquals("/path_with_underscore/file-with-dash.txt", location);
  }

  @Test
  @SneakyThrows
  void testGetFilesetFullName() {
    String name =
        GVFSUtils.getFilesetFullName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/a/b/c"));
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", name);

    name =
        GVFSUtils.getFilesetFullName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/"));
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", name);

    name =
        GVFSUtils.getFilesetFullName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", name);
  }

  @Test
  @SneakyThrows
  void testGetFilesetFullNameWithComplexNames() {
    String name =
        GVFSUtils.getFilesetFullName(
            new URI("gvfs://fileset/prod_catalog_2024/analytics_v2/user_events_fileset/data/"));
    Assertions.assertEquals("prod_catalog_2024.analytics_v2.user_events_fileset", name);
  }

  @Test
  @SneakyThrows
  void testInvalidGVFSPathMissingComponents() {
    // Path with only 2 components (missing fileset)
    URI invalidUri = new URI("gvfs://fileset/catalog/schema");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GVFSUtils.getFilesetFullName(invalidUri));

    assertThat(exception.getMessage())
        .contains("Invalid GVFS path")
        .contains("catalog/schema")
        .contains("Expected format: gvfs://fileset/catalog/schema/fileset")
        .contains("at least catalog, schema, and fileset components");
  }

  @Test
  @SneakyThrows
  void testInvalidGVFSPathOnlyOnePart() {
    // Path with only 1 component
    URI invalidUri = new URI("gvfs://fileset/catalog");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GVFSUtils.getFilesetFullName(invalidUri));

    assertThat(exception.getMessage())
        .contains("Invalid GVFS path")
        .contains("catalog")
        .contains("Expected format");
  }

  @Test
  @SneakyThrows
  void testInvalidGVFSPathEmpty() {
    // Empty path
    URI invalidUri = new URI("gvfs://fileset/");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GVFSUtils.getFilesetFullName(invalidUri));

    assertThat(exception.getMessage()).contains("Invalid GVFS path");
  }

  @Test
  @SneakyThrows
  void testInvalidGVFSLocationMissingComponents() {
    // Path with only 2 components
    URI invalidUri = new URI("gvfs://fileset/catalog/schema");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> GVFSUtils.getGVFSLocationSubpath(invalidUri));

    assertThat(exception.getMessage())
        .contains("Invalid GVFS path")
        .contains("Expected format: gvfs://fileset/catalog/schema/fileset");
  }

  @Test
  @SneakyThrows
  void testGVFSPathWithNumbers() {
    String name =
        GVFSUtils.getFilesetFullName(
            new URI("gvfs://fileset/catalog123/schema456/fileset789/data/"));
    Assertions.assertEquals("catalog123.schema456.fileset789", name);
  }

  @Test
  @SneakyThrows
  void testGVFSPathWithHyphensAndUnderscores() {
    String name =
        GVFSUtils.getFilesetFullName(
            new URI("gvfs://fileset/my-catalog/my_schema/my-fileset_v2/data/"));
    Assertions.assertEquals("my-catalog.my_schema.my-fileset_v2", name);
  }

  @Test
  @SneakyThrows
  void testGVFSLocationPreservesTrailingSlash() {
    // With trailing slash
    String location1 =
        GVFSUtils.getGVFSLocationSubpath(
            new URI("gvfs://fileset/catalog/schema/fileset/data/2024/"));
    Assertions.assertEquals("/data/2024/", location1);

    // Without trailing slash
    String location2 =
        GVFSUtils.getGVFSLocationSubpath(
            new URI("gvfs://fileset/catalog/schema/fileset/data/2024"));
    Assertions.assertEquals("/data/2024", location2);
  }

  @Test
  @SneakyThrows
  void testGVFSPathWithMinimalComponents() {
    // Exactly 3 components - minimum valid path
    String name = GVFSUtils.getFilesetFullName(new URI("gvfs://fileset/cat/sch/fil"));
    Assertions.assertEquals("cat.sch.fil", name);

    String location = GVFSUtils.getGVFSLocationSubpath(new URI("gvfs://fileset/cat/sch/fil"));
    Assertions.assertEquals("/", location);
  }
}
