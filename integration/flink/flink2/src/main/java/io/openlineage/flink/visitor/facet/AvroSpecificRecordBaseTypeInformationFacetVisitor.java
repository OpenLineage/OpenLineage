/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static io.openlineage.flink.utils.AvroSchemaUtils.convert;

import io.openlineage.client.OpenLineage.DatasetFacetsBuilder;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.converter.LineageDatasetWithIdentifier;
import io.openlineage.flink.util.TypeDatasetFacetUtil;
import java.lang.reflect.InvocationTargetException;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;

@Slf4j
public class AvroSpecificRecordBaseTypeInformationFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  public AvroSpecificRecordBaseTypeInformationFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    if (!TypeDatasetFacetUtil.isOnClasspath()) {
      return false;
    }

    Optional<TypeDatasetFacet> typeDatasetFacet =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset());

    // TODO: modify test
    return typeDatasetFacet
        .filter(datasetFacet -> datasetFacet.getTypeInformation() != null)
        .map(TypeDatasetFacet::getTypeInformation)
        .map(TypeInformation::getTypeClass)
        .filter(SpecificRecordBase.class::isAssignableFrom)
        .isPresent();
  }

  @Override
  public void apply(LineageDatasetWithIdentifier dataset, DatasetFacetsBuilder builder) {
    Optional<TypeDatasetFacet> typeDatasetFacet =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset());
    TypeInformation typeInformation =
        typeDatasetFacet.map(TypeDatasetFacet::getTypeInformation).orElse(null);

    getAvroSchema(typeInformation)
        .ifPresent(schema -> builder.schema(convert(context.getOpenLineage(), schema)));
  }

  private Optional<Schema> getAvroSchema(TypeInformation typeInformation) {
    try {
      Object schema =
          MethodUtils.invokeStaticMethod(typeInformation.getTypeClass(), "getClassSchema", null);
      return Optional.ofNullable(schema).filter(Schema.class::isInstance).map(Schema.class::cast);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      log.warn("Calling getClassSchema failed", e);
      return Optional.empty();
    }
  }
}
