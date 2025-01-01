/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.extension;

public interface OpenLineageExtensionProvider {

  String shadedPackage();

  default String getVisitorClassName() {
    return shadedPackage() + ".extension.v1.lifecycle.plan.SparkOpenLineageExtensionVisitor";
  }
}
