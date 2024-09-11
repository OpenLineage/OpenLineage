/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFieldsBuilder;
import io.openlineage.flink.client.OpenLineageContext;
import io.openlineage.flink.util.KafkaDatasetFacetUtil;
import io.openlineage.flink.util.TypeDatasetFacetUtil;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.streaming.api.lineage.LineageDataset;

/** Class used to extract type information from the facets returned by the collector */
@Slf4j
public class TypeInformationFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  public TypeInformationFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDataset flinkDataset) {
    if (!TypeDatasetFacetUtil.isOnClasspath()) {
      return false;
    }

    return KafkaDatasetFacetUtil.getFacet(flinkDataset).isPresent();
  }

  @Override
  public void apply(LineageDataset flinkDataset, OpenLineage.DatasetFacetsBuilder builder) {
    TypeInformation typeInformation =
        TypeDatasetFacetUtil.getFacet(flinkDataset).map(f -> f.getTypeInformation()).get();

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
