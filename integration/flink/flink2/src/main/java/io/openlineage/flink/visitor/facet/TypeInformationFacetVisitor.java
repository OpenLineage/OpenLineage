/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import io.openlineage.flink.util.KafkaDatasetFacetUtil;
import io.openlineage.flink.util.TypeDatasetFacetUtil;
import io.openlineage.flink.visitor.facet.schema.GenericTypeInfoSchemaBuilder;
import io.openlineage.flink.visitor.facet.schema.PojoTypeInfoSchemaBuilder;
import io.openlineage.flink.visitor.facet.schema.TypeInformationSchemaBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;

/** Class used to extract type information from the facets returned by the collector */
@Slf4j
public class TypeInformationFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;
  private final List<TypeInformationSchemaBuilder> schemaBuilders;

  public TypeInformationFacetVisitor(OpenLineageContext context) {
    this.context = context;
    this.schemaBuilders =
        Arrays.asList(new GenericTypeInfoSchemaBuilder(), new PojoTypeInfoSchemaBuilder());
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    // need to check class existence on classpath to make sure that flink-connector-kafka
    // package on the classpath supports lineage extraction
    if (!TypeDatasetFacetUtil.isOnClasspath()) {
      return false;
    }

    return KafkaDatasetFacetUtil.getFacet(dataset.getFlinkDataset()).isPresent();
  }

  @Override
  public void apply(
      LineageDatasetWithIdentifier dataset, OpenLineage.DatasetFacetsBuilder builder) {
    TypeInformation typeInformation =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset())
            .map(f -> f.getTypeInformation())
            .orElse(null);

    Optional<TypeInformationSchemaBuilder> schemaBuilder =
        schemaBuilders.stream().filter(b -> b.isDefinedAt(typeInformation)).findFirst();

    if (schemaBuilder.isPresent()) {
      builder.schema(
          context
              .getOpenLineage()
              .newSchemaDatasetFacetBuilder()
              .fields(schemaBuilder.get().buildSchemaFields(typeInformation))
              .build());
    } else {
      log.warn("Could not extract schema from type {}", typeInformation);
    }
  }
}
