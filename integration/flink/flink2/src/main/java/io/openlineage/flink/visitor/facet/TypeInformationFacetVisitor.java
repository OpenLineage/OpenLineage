/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFieldsBuilder;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import io.openlineage.flink.util.KafkaDatasetFacetUtil;
import io.openlineage.flink.util.TypeDatasetFacetUtil;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;

/** Class used to extract type information from the facets returned by the collector */
@Slf4j
public class TypeInformationFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  public TypeInformationFacetVisitor(OpenLineageContext context) {
    this.context = context;
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

    // TODO: support GenericAvroRecord & support protobuf
    if (typeInformation instanceof GenericTypeInfo) {
      builder.schema(
          context
              .getOpenLineage()
              .newSchemaDatasetFacetBuilder()
              .fields(from((GenericTypeInfo) typeInformation))
              .build());
    } else {
      log.warn("Could not extract schema from type {}", typeInformation);
    }
  }

  private List<SchemaDatasetFacetFields> from(GenericTypeInfo genericTypeInfo) {
    return Arrays.stream(genericTypeInfo.getTypeClass().getFields())
        .filter(f -> Modifier.isPublic(f.getModifiers()))
        .map(
            f ->
                new SchemaDatasetFacetFieldsBuilder()
                    .type(
                        f.getType()
                            .getSimpleName()) // TODO: go deeper to extract fields recursively
                    .name(f.getName())
                    .build())
        .collect(Collectors.toList());
  }
}
