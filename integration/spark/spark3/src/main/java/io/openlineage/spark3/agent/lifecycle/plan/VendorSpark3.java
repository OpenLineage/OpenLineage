/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.lifecycle.plan;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ExpressionDependencyVisitor;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface VendorSpark3 {

  List<String> VENDORS =
      Arrays.asList(
          // Add vendor classes here
          "io.openlineage.spark.vendor.iceberg.IcebergVendorSpark3");

  static Collection<VendorSpark3> getVendors() {
    return getVendors(Collections.emptyList());
  }

  static Collection<VendorSpark3> getVendors(List<String> additionalVendors) {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();

    List<VendorSpark3> vendors =
        Stream.concat(VENDORS.stream(), additionalVendors.stream())
            .map(
                vendorClassName -> {
                  try {
                    Class<?> vendor = cl.loadClass(vendorClassName);
                    return (VendorSpark3) vendor.newInstance();
                  } catch (ClassNotFoundException
                      | InstantiationException
                      | IllegalAccessException e) {
                    return null;
                  }
                })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    return vendors;
  }

  default Optional<CatalogHandler> getCatalogHandler(OpenLineageContext context) {
    return Optional.empty();
  }

  default Optional<ExpressionDependencyVisitor> getExpressionDependencyVisitor() {
    return Optional.empty();
  }
}
