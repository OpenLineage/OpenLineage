/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension;

import io.openlineage.unshaded.spark.extension.v1.OpenLineageExtensionProvider;

public class TestOpenLineageExtensionProvider implements OpenLineageExtensionProvider {
  @Override
  public String shadedPackage() {
    return "io.openlineage.shaded";
  }
}
