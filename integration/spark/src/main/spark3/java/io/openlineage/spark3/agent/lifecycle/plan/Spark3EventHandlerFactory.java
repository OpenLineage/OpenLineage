package io.openlineage.spark3.agent.lifecycle.plan;

import com.google.common.collect.ImmutableList;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.DatasetVersionDatasetFacet;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.OpenLineageEventHandlerFactory;
import io.openlineage.spark3.agent.facets.builder.DatasetVersionOutputDatasetFacetBuilder;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import lombok.AllArgsConstructor;
import org.apache.spark.package$;

public class Spark3EventHandlerFactory implements OpenLineageEventHandlerFactory {
  @Override
  public List<CustomFacetBuilder<?, ? extends OutputDatasetFacet>> createOutputDatasetFacetBuilders(
      OpenLineageContext context) {
    if (package$.MODULE$.SPARK_VERSION().startsWith("2")) {
      return Collections.emptyList();
    }
    return ImmutableList.of(
        new DatasetVersionFacetConverter<>(
            context, new DatasetVersionOutputDatasetFacetBuilder(context)));
  }

  @AllArgsConstructor
  private static class DatasetVersionFacetConverter<T>
      extends CustomFacetBuilder<T, OpenLineage.OutputDatasetFacet> {
    private final OpenLineageContext context;
    private final CustomFacetBuilder<T, OpenLineage.DatasetFacet> parent;

    @Override
    public boolean isDefinedAt(Object x) {
      return parent.isDefinedAt(x);
    }

    @Override
    protected void build(T event, BiConsumer<String, ? super OutputDatasetFacet> consumer) {
      parent.accept(event, wrappingConsumer(consumer));
    }

    private BiConsumer<String, DatasetFacet> wrappingConsumer(
        BiConsumer<String, ? super OutputDatasetFacet> consumer) {
      return (s, x) -> {
        OutputDatasetFacet facet = context.getOpenLineage().newOutputDatasetFacet();
        facet
            .getAdditionalProperties()
            .put("datasetVersion", ((DatasetVersionDatasetFacet) x).getDatasetVersion());
        consumer.accept("datasetVersion", facet);
      };
    }
  }
}
