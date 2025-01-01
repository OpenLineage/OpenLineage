/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark.api.Vendor;
import io.openlineage.spark.api.Vendors;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.junit.jupiter.api.Test;
import scala.PartialFunction;

@SuppressWarnings("PMD.JUnit5TestShouldBePackagePrivate")
public class VendorsTest implements Vendor {

  static class VisitorFactoryTest implements VisitorFactory {

    @Override
    public List<PartialFunction<LogicalPlan, List<OpenLineage.InputDataset>>> getInputVisitors(
        OpenLineageContext context) {
      return Collections.emptyList();
    }

    @Override
    public List<PartialFunction<LogicalPlan, List<OpenLineage.OutputDataset>>> getOutputVisitors(
        OpenLineageContext context) {
      return Collections.emptyList();
    }
  }

  static class OpenLineageEventHandlerFactoryTest implements OpenLineageEventHandlerFactory {}

  @Override
  public boolean isVendorAvailable() {
    return true;
  }

  @Override
  public Optional<VisitorFactory> getVisitorFactory() {
    return Optional.of(new VisitorFactoryTest());
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

    assertFalse(vendors.getVisitorFactories().isEmpty());
    assertTrue(
        vendors.getEventHandlerFactories().stream()
            .anyMatch(v -> v instanceof OpenLineageEventHandlerFactoryTest));
    assertTrue(
        vendors.getVisitorFactories().stream().anyMatch(v -> v instanceof VisitorFactoryTest));

    assertTrue(Vendors.empty().getVisitorFactories().isEmpty());
    assertTrue(Vendors.empty().getEventHandlerFactories().isEmpty());
  }
}
