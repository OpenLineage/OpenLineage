/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import java.util.Optional;

public interface Vendor {

  /**
   * This method must be implemented, and It will be called to check if the Vendor is available If
   * return true means that the other methods can be called and will have all the classes necessary
   * to work in the environment.
   *
   * @return
   */
  boolean isVendorAvailable();

  /**
   * Implement if the vendor needs to add components and facets from Spark events.
   *
   * @return
   */
  default Optional<OpenLineageEventHandlerFactory> getEventHandlerFactory() {
    return Optional.empty();
  }
}
