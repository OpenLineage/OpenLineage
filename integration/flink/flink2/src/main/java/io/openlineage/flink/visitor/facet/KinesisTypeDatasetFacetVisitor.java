/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFieldsBuilder;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
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
import org.apache.flink.streaming.api.lineage.LineageDatasetFacet;

/**
 * Visitor to extract type information from the Kinesis connector's {@code type} lineage facet
 * ({@code org.apache.flink.connector.kinesis.lineage.TypeDatasetFacet}, added in FLINK-39813) and
 * convert it into an OpenLineage schema facet.
 *
 * <p>The facet is accessed reflectively so that this integration does not require a compile-time
 * dependency on the Kinesis connector. When the connector's type information is Avro-based (e.g.
 * records deserialized via AWS Glue Schema Registry), the Avro delegate extracts the full
 * field-level schema — the same behavior the Kafka type facet receives.
 */
@Slf4j
public class KinesisTypeDatasetFacetVisitor implements DatasetFacetVisitor {

  private static final String KINESIS_TYPE_FACET_CLASS =
      "org.apache.flink.connector.kinesis.lineage.TypeDatasetFacet";
  private static final String TYPE_FACET_NAME = "type";

  private final OpenLineageContext context;
  private final AvroTypeDatasetFacetVisitorDelegate avroDelegate;

  public KinesisTypeDatasetFacetVisitor(OpenLineageContext context) {
    this.context = context;
    if (AvroTypeDatasetFacetVisitorDelegate.isApplicable()) {
      avroDelegate = new AvroTypeDatasetFacetVisitorDelegate(context);
    } else {
      avroDelegate = null;
    }
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    return getTypeInformation(dataset).isPresent();
  }

  @Override
  public void apply(
      LineageDatasetWithIdentifier dataset, OpenLineage.DatasetFacetsBuilder builder) {
    TypeInformation<?> typeInformation = getTypeInformation(dataset).orElse(null);
    if (typeInformation == null) {
      return;
    }

    if (avroDelegate != null && avroDelegate.isDefinedAt(typeInformation)) {
      avroDelegate.delegate(typeInformation).ifPresent(builder::schema);
    } else if (typeInformation instanceof GenericTypeInfo
        || typeInformation instanceof PojoTypeInfo) {
      builder.schema(
          context
              .getOpenLineage()
              .newSchemaDatasetFacetBuilder()
              .fields(fromFields(typeInformation.getTypeClass().getFields()))
              .build());
    }
  }

  private Optional<TypeInformation<?>> getTypeInformation(LineageDatasetWithIdentifier dataset) {
    LineageDatasetFacet facet = dataset.getFlinkDataset().facets().get(TYPE_FACET_NAME);

    if (facet == null || !KINESIS_TYPE_FACET_CLASS.equals(facet.getClass().getName())) {
      return Optional.empty();
    }

    try {
      Object typeInformation = facet.getClass().getMethod("getTypeInformation").invoke(facet);
      if (typeInformation instanceof TypeInformation) {
        return Optional.of((TypeInformation<?>) typeInformation);
      }
    } catch (ReflectiveOperationException e) {
      log.warn("Could not extract type information from Kinesis type facet", e);
    }
    return Optional.empty();
  }

  private List<SchemaDatasetFacetFields> fromFields(Field... fields) {
    return Arrays.stream(fields)
        .filter(f -> Modifier.isPublic(f.getModifiers()))
        .filter(field -> !Modifier.isStatic(field.getModifiers()))
        .map(
            f ->
                new SchemaDatasetFacetFieldsBuilder()
                    .type(f.getType().getSimpleName())
                    .name(f.getName())
                    .build())
        .collect(Collectors.toList());
  }
}
