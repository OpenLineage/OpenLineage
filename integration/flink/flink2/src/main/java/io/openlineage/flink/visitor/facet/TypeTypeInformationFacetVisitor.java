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
import io.openlineage.flink.util.TypeDatasetFacetUtil;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;

/** Class used to extract type information from the facets returned by the collector */
@Slf4j
public class TypeTypeInformationFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  public TypeTypeInformationFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    // need to check class existence on classpath to make sure that flink-connector-kafka
    // package on the classpath supports lineage extraction
    if (!TypeDatasetFacetUtil.isOnClasspath()) {
      return false;
    }

    Optional<TypeDatasetFacet> typeDatasetFacet =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset());

    return typeDatasetFacet
        .filter(datasetFacet -> datasetFacet.getTypeInformation() != null)
        .map(TypeDatasetFacet::getTypeInformation)
        .map(TypeInformation::getTypeClass)
        .filter(
            c ->
                GenericTypeInfo.class.isAssignableFrom(c) || PojoTypeInfo.class.isAssignableFrom(c))
        .isPresent();
  }

  @Override
  public void apply(
      LineageDatasetWithIdentifier dataset, OpenLineage.DatasetFacetsBuilder builder) {
    Optional<TypeDatasetFacet> typeDatasetFacet =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset());
    TypeInformation typeInformation =
        typeDatasetFacet.map(TypeDatasetFacet::getTypeInformation).orElse(null);

    builder.schema(
        context
            .getOpenLineage()
            .newSchemaDatasetFacetBuilder()
            .fields(fromFields(typeInformation.getTypeClass().getFields()))
            .build());
  }

  private List<SchemaDatasetFacetFields> fromFields(Field... fields) {
    return Arrays.stream(fields)
        .filter(f -> Modifier.isPublic(f.getModifiers()))
        .filter(field -> !Modifier.isStatic(field.getModifiers()))
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
