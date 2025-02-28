/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension;

public class TestOpenLineageExtensionProvider implements OpenLineageExtensionProvider {
  @Override
  public String shadedPackage() {
    return "io.openlineage.spark.shade";
  }
}
