/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Optional;
import lombok.SneakyThrows;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;

class HudiUtilsTest {

  private static final String BASE_PATH = "s3://bucket/warehouse/orders";

  @Test
  void testBasePathFromRelation() {
    assertEquals(URI.create(BASE_PATH), HudiUtils.basePath(new HadoopPathRelation(), new Object()));
  }

  @Test
  void testBasePathFromHudi1MetaClientStoragePath() {
    // Hudi 1.x removed getBasePathV2() and getBasePath() returns a StoragePath
    assertEquals(URI.create(BASE_PATH), HudiUtils.basePath(new Object(), new Hudi1MetaClient()));
  }

  @Test
  void testBasePathFromHudi0MetaClientString() {
    assertEquals(URI.create(BASE_PATH), HudiUtils.basePath(new Object(), new Hudi0MetaClient()));
  }

  @Test
  void testBasePathThrowsWhenUnresolvable() {
    assertThrows(IllegalStateException.class, () -> HudiUtils.basePath(new Object(), new Object()));
  }

  @Test
  void testBasePathOfRelationDoesNotNeedMetaClient() {
    assertEquals(Optional.of(URI.create(BASE_PATH)), HudiUtils.basePath(new HadoopPathRelation()));
  }

  @Test
  void testBasePathOfRelationFromMetaClient() {
    assertEquals(
        Optional.of(URI.create(BASE_PATH)), HudiUtils.basePath(new RelationWithMetaClient()));
  }

  @Test
  void testBasePathOfRelationIsEmptyForNullMetaClient() {
    assertFalse(HudiUtils.basePath(new RelationWithNullMetaClient()).isPresent());
  }

  @Test
  void testBasePathOfRelationIsEmptyWhenUnresolvable() {
    assertFalse(HudiUtils.basePath(new Object()).isPresent());
  }

  @Test
  void testIsInstanceOfMatchesClassAndSubclasses() {
    assertTrue(HudiUtils.isInstanceOf(new Base(), Base.class.getName()));
    assertTrue(HudiUtils.isInstanceOf(new Sub(), Base.class.getName()));
    assertFalse(HudiUtils.isInstanceOf(new Base(), Sub.class.getName()));
    assertFalse(HudiUtils.isInstanceOf(new Object(), Base.class.getName()));
    assertFalse(HudiUtils.isInstanceOf(null, Base.class.getName()));
  }

  @Test
  @SneakyThrows
  void testIsInstanceOfMatchesClassFromAnotherClassLoader() {
    // e.g. Hudi added with --packages: its classes aren't visible from HudiUtils's classloader
    URL testClasses = Sub.class.getProtectionDomain().getCodeSource().getLocation();
    try (URLClassLoader loader = new URLClassLoader(new URL[] {testClasses}, null)) {
      Object sub = loader.loadClass(Sub.class.getName()).getDeclaredConstructor().newInstance();

      assertFalse(sub instanceof Base);
      assertTrue(HudiUtils.isInstanceOf(sub, Base.class.getName()));
    }
  }

  public static class Base {}

  public static class Sub extends Base {}

  public static class HadoopPathRelation {
    public Path basePath() {
      return new Path(BASE_PATH);
    }
  }

  public static class Hudi1MetaClient {
    public StoragePath getBasePath() {
      return new StoragePath(BASE_PATH);
    }
  }

  public static class RelationWithMetaClient {
    public Hudi1MetaClient metaClient() {
      return new Hudi1MetaClient();
    }
  }

  public static class RelationWithNullMetaClient {
    public Object metaClient() {
      return null;
    }
  }

  public static class Hudi0MetaClient {
    public String getBasePath() {
      return BASE_PATH;
    }
  }
}
