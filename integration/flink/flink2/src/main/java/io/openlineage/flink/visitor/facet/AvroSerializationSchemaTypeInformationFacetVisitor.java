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
import io.openlineage.flink.utils.ClassUtils;
import io.openlineage.flink.visitor.wrapper.AvroUtils;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.MethodUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.kafka.lineage.TypeDatasetFacet;

/**
 * Class used to extract serialization schema from the facets returned by lineage interfaces.
 * Serialization schema can be extracted from the {@link TypeDatasetFacet}. However, this requires
 * extra method within the facet interface, which was added in
 * https://github.com/apache/flink-connector-kafka/pull/171.
 *
 * <p>It's safer to check through the reflection if the method exists and if it does.
 *
 * <p>It is used to extract the serialization schema from the {@link TypeDatasetFacet} and convert
 * it to {@link io.openlineage.client.OpenLineage.SchemaDatasetFacet}
 */
@Slf4j
public class AvroSerializationSchemaTypeInformationFacetVisitor implements DatasetFacetVisitor {

  private final OpenLineageContext context;

  public AvroSerializationSchemaTypeInformationFacetVisitor(OpenLineageContext context) {
    this.context = context;
  }

  /**
   * Checks if {@link TypeTypeInformationFacetVisitor} contains serialization schema method in the
   * TypeDatasetFacet interface.
   *
   * @return
   */
  public static boolean isApplicable() {
    Method method =
        MethodUtils.getAccessibleMethod(TypeDatasetFacet.class, "getSerializationSchema");

    // TODO: test this is not applicable when no avro classes are on the classpath
    return method != null && ClassUtils.hasAvroClasses();
  }

  @Override
  public boolean isDefinedAt(LineageDatasetWithIdentifier dataset) {
    // need to check class existence on classpath to make sure that flink-connector-kafka
    // package on the classpath supports lineage extraction
    log.info("AvroSerializationSchemaFacetVisitor isDefinedAt: {}", dataset);
    if (!TypeDatasetFacetUtil.isOnClasspath()) {
      return false;
    }

    Optional<TypeDatasetFacet> typeDatasetFacet =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset());

    if (typeDatasetFacet.isEmpty()) {
      return false;
    }

    Optional<SerializationSchema> serializationSchema =
        getSerializationSchema(typeDatasetFacet.get());

    return AvroUtils.getAvroSchema(serializationSchema).isPresent();
  }

  @Override
  public void apply(LineageDatasetWithIdentifier dataset, DatasetFacetsBuilder builder) {
    log.info("Applying Avro serialization schema: {}", dataset.getFlinkDataset());
    Optional<TypeDatasetFacet> typeDatasetFacet =
        TypeDatasetFacetUtil.getFacet(dataset.getFlinkDataset());

    AvroUtils.getAvroSchema(getSerializationSchema(typeDatasetFacet.get()))
        .map(schema -> convert(context.getOpenLineage(), schema))
        .ifPresent(builder::schema);
  }

  private Optional<SerializationSchema> getSerializationSchema(TypeDatasetFacet typeDatasetFacet) {
    try {
      return (Optional<SerializationSchema>)
          MethodUtils.invokeMethod(typeDatasetFacet, "getSerializationSchema");
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      log.error("Failed to get serialization schema", e);
      throw new RuntimeException(e);
    }
  }

  // TODO: write unit test once the method is added to the interface so that reflection is not
  // tested
}
