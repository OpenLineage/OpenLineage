/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import io.openlineage.spark.api.Vendors;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.Test;

@SuppressWarnings("PMD.JUnit5TestShouldBePackagePrivate")
public class VendorsTest implements Vendor {

  static class OpenLineageEventHandlerFactoryTest implements OpenLineageEventHandlerFactory {}

  @Override
  public boolean isVendorAvailable() {
    return true;
  }

  @Override
  public Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.of(new OpenLineageEventHandlerFactoryTest());
  }

  @Test
  void testGetVendors() {

    Vendors vendors =
        Vendors.getVendors(
            Collections.singletonList("io.openlineage.spark.agent.util.VendorsTest"));

    assertTrue(
        vendors.getEventHandlerFactories().stream()
            .anyMatch(v -> v instanceof OpenLineageEventHandlerFactoryTest));
    assertTrue(Vendors.empty().getEventHandlerFactories().isEmpty());
  }
}
