/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.gcp;

import io.openlineage.spark.agent.vendor.gcp.lifecycle.plan.GcpEventHandlerFactory;
import io.openlineage.spark.agent.vendor.gcp.util.GCPUtils;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import java.util.Optional;

public class GcpVendor implements Vendor {

  @Override
  public boolean isVendorAvailable() {
    return GCPUtils.isDataprocRuntime();
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new GcpEventHandlerFactory());
  }
}
