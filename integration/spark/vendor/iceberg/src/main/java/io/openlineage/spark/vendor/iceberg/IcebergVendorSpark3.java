/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.vendor.iceberg;

import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.vendor.iceberg.agent.lifecycle.plan.catalog.IcebergHandler;
import io.openlineage.spark.vendor.iceberg.agent.lifecycle.plan.column.visitors.IcebergMergeIntoDependencyVisitor;
import io.openlineage.spark3.agent.lifecycle.plan.VendorSpark3;
import io.openlineage.spark3.agent.lifecycle.plan.catalog.CatalogHandler;
import io.openlineage.spark3.agent.lifecycle.plan.column.visitors.ExpressionDependencyVisitor;
import java.util.Optional;

public class IcebergVendorSpark3 implements VendorSpark3 {
  @Override
  public Optional<CatalogHandler> getCatalogHandler(OpenLineageContext context) {
    CatalogHandler icebergHandler = new IcebergHandler(context);
    if (icebergHandler.hasClasses()) {
      return Optional.of(icebergHandler);
    } else {
      return VendorSpark3.super.getCatalogHandler(context);
    }
  }

  @Override
  public Optional<ExpressionDependencyVisitor> getExpressionDependencyVisitor() {
    return Optional.of(new IcebergMergeIntoDependencyVisitor());
  }
}
