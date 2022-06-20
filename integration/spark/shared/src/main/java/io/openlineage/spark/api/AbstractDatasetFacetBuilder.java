/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.api;

import io.openlineage.client.OpenLineage;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Abstract base class for {@link scala.PartialFunction}s that return a {@link
 * OpenLineage.DatasetFacet}.
 *
 * @see OpenLineageEventHandlerFactory for a list of event types that may be passed to this
 *     function.
 * @param <T>
 */
@RequiredArgsConstructor
public abstract class AbstractDatasetFacetBuilder<T>
    extends AbstractGenericArgPartialFunction<T, OpenLineage.DatasetFacet> {
  @NonNull protected final OpenLineageContext context;
}
