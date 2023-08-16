/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.proxy.api.models;

import lombok.NonNull;

/**
 * LineageStream provides the generic implementation of the backend destinations supported by the
 * proxy backend.
 */
public abstract class LineageStream {
  /**
   * The Type enum (and JsonSubTypes above) are extended for each new type of destination that the
   * proxy backend supports. There is a subtype class for each of these destination types.
   */
  enum Type {
    CONSOLE,
    HTTP,
    KAFKA
  }

  private final Type type; // NOPMD
  /**
   * The constructor sets up the type for destination for logging purposes.
   *
   * @param type type of destination implemented by the subtype.
   */
  LineageStream(@NonNull final Type type) {
    this.type = type;
  }

  /**
   * This is the method that is called when a new lineage event is emitted from the data platform.
   * The specific destination class implements this method with the logic to send the event to its
   * supported destination.
   *
   * @param eventAsString the OpenLineage event as a {code string} value
   */
  public abstract void collect(String eventAsString);
}
