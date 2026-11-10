/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.fabric;

import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;

/**
 * Microsoft Fabric. It needs no extra classes, so it is always available; what it adds only applies
 * in Fabric sessions.
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
