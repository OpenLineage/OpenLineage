/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URI;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;

class HudiUtilsTest {

  @Test
  void testBasePathFromRelation() {
    assertEquals(
        URI.create("s3://bucket/warehouse/orders"),
        HudiUtils.basePath(new HadoopPathRelation(), new Object()));
  }

  @Test
  void testBasePathFromHudi1MetaClientStoragePath() {
    // Hudi 1.x removed getBasePathV2() and getBasePath() returns a StoragePath
    assertEquals(
        URI.create("s3://bucket/warehouse/orders"),
        HudiUtils.basePath(new Object(), new Hudi1MetaClient()));
  }

  @Test
  void testBasePathFromHudi0MetaClientString() {
    assertEquals(
        URI.create("s3://bucket/warehouse/orders"),
        HudiUtils.basePath(new Object(), new Hudi0MetaClient()));
  }

  @Test
  void testBasePathThrowsWhenUnresolvable() {
    assertThrows(IllegalStateException.class, () -> HudiUtils.basePath(new Object(), new Object()));
  }

  public static class HadoopPathRelation {
    public Path basePath() {
      return new Path("s3://bucket/warehouse/orders");
    }
  }

  public static class Hudi1MetaClient {
    public StoragePath getBasePath() {
      return new StoragePath("s3://bucket/warehouse/orders");
    }
  }

  public static class Hudi0MetaClient {
    public String getBasePath() {
      return "s3://bucket/warehouse/orders";
    }
  }
}
