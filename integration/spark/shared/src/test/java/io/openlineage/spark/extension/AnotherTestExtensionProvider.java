/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension;

/** A provider which is not registered in the services file of the test resources. */
public class AnotherTestExtensionProvider implements OpenLineageExtensionProvider {
  @Override
  public String shadedPackage() {
    return "io.openlineage.spark.shade";
  }
}
