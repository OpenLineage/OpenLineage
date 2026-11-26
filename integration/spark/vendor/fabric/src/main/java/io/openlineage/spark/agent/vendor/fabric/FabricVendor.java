/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;

/**
 * Microsoft Fabric. The vendor is always loaded, because Spark jobs outside Fabric can also read
 * and write Fabric warehouses and OneLake. Code that needs a Fabric session checks for one itself.
 */
public class FabricVendor implements Vendor {

  @Override
  public boolean isVendorAvailable() {
    return true;
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new FabricEventHandlerFactory());
  }
}
