/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent;

import java.util.UUID;
import lombok.experimental.UtilityClass;

/** Identifier helpers for tests. */
@UtilityClass
public class TestIds {

  /**
   * A random 32-character hex string (a UUID with the dashes removed), for unique names like test
   * catalogs, tables, directories, or run ids.
   */
  public String randomHex() {
    return UUID.randomUUID().toString().replace("-", "");
  }

  /** {@code prefix} followed by {@link #randomHex()}, e.g. {@code randomName("test_table_")}. */
  public String randomName(String prefix) {
    return prefix + randomHex();
  }
}
