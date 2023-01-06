/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * {@link CustomFacetBuilder} that generates {@link OpenLineage.RunFacet}s from input events.
 *
 * @see OpenLineageEventHandlerFactory for a list of event types that may be passed to this builder.
 * @param <T>
 */
@RequiredArgsConstructor
public abstract class AbstractRunFacetBuilder<T>
    extends CustomFacetBuilder<T, OpenLineage.RunFacet> {
  @NonNull protected final OpenLineageContext context;
}
