/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFieldsBuilder;
import io.openlineage.flink.client.OpenLineageContext;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.kafka.lineage.facets.TypeInformationFacet;
import org.apache.flink.streaming.api.lineage.LineageDataset;

/** Class used to extract type information from the facets returned by the collector */
@Slf4j
public class TypeInformationFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  /**
   * Class name as string is required as the code should work even though classes may be not present
   * on the classpath.
   */
  private static String TYPE_INFORMATION_FACET_CLASS_NAME =
      "org.apache.flink.connector.kafka.lineage.facets.TypeInformationFacet";

  private final Optional<Class> typeInformationFacetClass;

  public TypeInformationFacetVisitor(OpenLineageContext context) {
    this.context = context;

    Class aClass = null;
    try {
      aClass = Class.forName(TYPE_INFORMATION_FACET_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      log.debug("Class {} not present on a classpath", TYPE_INFORMATION_FACET_CLASS_NAME);
    }

    typeInformationFacetClass = Optional.ofNullable(aClass);
  }

  /**
   * This method should work even if type information facet class not available on the classpath.
   *
   * @param flinkDataset
   * @return
   */
  @Override
  public boolean isDefinedAt(LineageDataset flinkDataset) {
    if (typeInformationFacetClass.isEmpty()) {
      return false;
    }
    return flinkDataset.facets().values().stream()
        .map(f -> f.getClass())
        .filter(c -> c.isAssignableFrom(typeInformationFacetClass.get()))
        .findFirst()
        .isPresent();
  }

  @Override
  public void apply(LineageDataset flinkDataset, OpenLineage.DatasetFacetsBuilder builder) {
    TypeInformation typeInformation =
        flinkDataset.facets().values().stream()
            .filter(c -> c.getClass().isAssignableFrom(typeInformationFacetClass.get()))
            .findFirst()
            .map(f -> (TypeInformationFacet) f)
            .map(f -> f.getTypeInformation())
            .get();

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
