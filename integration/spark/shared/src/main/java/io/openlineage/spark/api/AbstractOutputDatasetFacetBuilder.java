/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * {@link CustomFacetBuilder} that generates {@link OpenLineage.OutputDatasetFacet}s from input
 * events.
 *
 * @see OpenLineageEventHandlerFactory for a list of event types that may be passed to this builder.
 * @param <T>
 */
@RequiredArgsConstructor
public abstract class AbstractOutputDatasetFacetBuilder<T>
    extends CustomFacetBuilder<T, OpenLineage.OutputDatasetFacet> {
  @NonNull protected final OpenLineageContext context;
}
