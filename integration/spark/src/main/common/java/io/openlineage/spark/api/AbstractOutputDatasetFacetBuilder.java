package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * {@link CustomFacetBuilder} that generates {@link
 * io.openlineage.client.OpenLineage.OutputDatasetFacet}s from input events.
 *
 * @see io.openlineage.spark.api.OpenLineageEventHandlerFactory for a list of event types that may
 *     be passed to this builder.
 * @param <T>
 */
@RequiredArgsConstructor
public abstract class AbstractOutputDatasetFacetBuilder<T>
    extends CustomFacetBuilder<T, OpenLineage.OutputDatasetFacet> {
  @NonNull protected final OpenLineageContext context;
}
