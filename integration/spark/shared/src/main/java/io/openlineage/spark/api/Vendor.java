/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.spark.agent.lifecycle.VisitorFactory;
import java.util.Optional;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;

public interface Vendor {

  /**
   * This method must be implemented, and It will be called to check if the Vendor is available If
   * return true means that the other methods can be called and will have all the classes necessary
   * to work in the environment.
   *
   * @return
   */
  boolean isVendorAvailable();

  /** Implement if the vendor needs to add visitors for iterating on {@link LogicalPlan}. */
  default Optional<VisitorFactory> getVisitorFactory() {
    return Optional.empty();
  }

  /**
   * Implement if the vendor needs to add components and facets from Spark events.
   *
   * @return
   */
  default Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.empty();
  }
}
