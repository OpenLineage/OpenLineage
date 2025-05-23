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
public class TypeDatasetFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;
  private final AvroTypeDatasetFacetVisitorDelegate avroDelegate;

  public TypeDatasetFacetVisitor(OpenLineageContext context) {
    this.context = context;

    if (AvroTypeDatasetFacetVisitorDelegate.isApplicable()) {
      avroDelegate = new AvroTypeDatasetFacetVisitorDelegate(context);
    } else {
      avroDelegate = null;
    }
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    // need to check class existence on classpath to make sure that flink-connector-kafka
    // package on the classpath supports lineage extraction
    if (!TypeDatasetFacetUtil.isOnClasspath()) {
      return false;
    }

    return TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset()).isPresent();
  }

  @Override
  public void apply(
      LineageDatasetWithIdentifier dataset, OpenLineage.DatasetFacetsBuilder builder) {
    Optional<TypeDatasetFacet> typeDatasetFacet =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset());
    TypeInformation typeInformation =
        typeDatasetFacet.map(TypeDatasetFacet::getTypeInformation).orElse(null);

    Class typeClazz = typeInformation.getTypeClass();

    if (avroDelegate != null && avroDelegate.isDefinedAt(typeInformation)) {
      avroDelegate.delegate(typeInformation).ifPresent(builder::schema);
    } else if (typeInformation instanceof GenericTypeInfo
        || typeInformation instanceof PojoTypeInfo) {
      builder.schema(
          context
              .getOpenLineage()
              .newSchemaDatasetFacetBuilder()
              .fields(fromFields(typeClazz.getFields()))
              .build());
    }
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
