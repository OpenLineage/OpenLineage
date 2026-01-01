/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.iceberg;

import io.openlineage.spark.agent.vendor.iceberg.lifecycle.plan.IcebergEventHandlerFactory;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;

public class IcebergVendor implements Vendor {

  public static boolean hasIcebergClasses() {
    try {
      IcebergVendor.class.getClassLoader().loadClass("org.apache.iceberg.catalog.Catalog");
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public boolean isVendorAvailable() {
    return hasIcebergClasses();
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new IcebergEventHandlerFactory());
  }
}
