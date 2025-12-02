package io.openlineage.client.utils;

import io.openlineage.client.utils.filesystem.gvfs.GVFSUtils;
import java.net.URI;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
  void testGetGVFSLocation() {
    String location =
        GVFSUtils.getGVFSLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/a/b/c"));
    Assertions.assertEquals("/a/b/c", location);

    location =
        GVFSUtils.getGVFSLocation(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/a/b/c/"));
    Assertions.assertEquals("/a/b/c/", location);

    location =
        GVFSUtils.getGVFSLocation(new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/"));
    Assertions.assertEquals("/", location);

    location =
        GVFSUtils.getGVFSLocation(new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));
    Assertions.assertEquals("/", location);
  }

  @Test
  @SneakyThrows
  void testGetGVFSIdentifierName() {
    String name =
        GVFSUtils.getGVFSIdentifierName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/a/b/c"));
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", name);

    name =
        GVFSUtils.getGVFSIdentifierName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name/"));
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", name);

    name =
        GVFSUtils.getGVFSIdentifierName(
            new URI("gvfs://fileset/catalog_name/schema_name/fileset_name"));
    Assertions.assertEquals("catalog_name.schema_name.fileset_name", name);
  }
}
