/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import com.google.protobuf.Descriptors.FileDescriptor;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;

/** Utility methods to deal with Protobuf serialization schemas */
@Slf4j
public class ProtobufUtils {

  /**
   * Given a serialization schema, the method verifies if a produced output type extends Protobuf
   * generated classes.
   *
   * @param serializationSchema
   * @return
   */
  public static boolean isProtobufSerializationSchema(SerializationSchema serializationSchema) {
    Optional<Class> parentClass = protobufParentClass();
    if (parentClass.isEmpty()) {
      log.debug("Protobuf parent Message class not loaded");
      return false;
    }

    return getProtobufSerializedClass(serializationSchema)
        .map(t -> parentClass.get().isAssignableFrom(t))
        .orElse(false);
  }

  public static Optional<OpenLineage.SchemaDatasetFacet> convert(
      OpenLineage openLineage, SerializationSchema serializationSchema) {
    Optional<Class> protobufSerializedClass = getProtobufSerializedClass(serializationSchema);

    if (protobufSerializedClass.isEmpty()) {
      return Optional.empty();
    }

    try {
      Class<?> staticDefintionClass =
          Class.forName(protobufSerializedClass.get().getCanonicalName() + "OuterClass");

      Field descriptorField = FieldUtils.getField(staticDefintionClass, "descriptor", true);
      FileDescriptor fileDescriptor = (FileDescriptor) descriptorField.get(staticDefintionClass);

      OpenLineage.SchemaDatasetFacetBuilder builder = openLineage.newSchemaDatasetFacetBuilder();
      List<SchemaDatasetFacetFields> facetFields = new LinkedList<>();

      fileDescriptor.getMessageTypes().stream()
          .flatMap(d -> d.getFields().stream())
          .filter(
              d ->
                  protobufSerializedClass.get().getName().contains(d.getContainingType().getName()))
          .forEach(
              protoField ->
                  facetFields.add(
                      openLineage.newSchemaDatasetFacetFields(
                          protoField.getName(),
                          protoField.getJavaType().name().toLowerCase(),
                          "")));

      return Optional.of(builder.fields(facetFields).build());
    } catch (ClassNotFoundException e) {
      // swallow it
      log.warn("Couldn't find OuterClass for {}: {}", protobufSerializedClass.get(), e);
      return Optional.empty();
    } catch (IllegalAccessException e) {
      // swallow it
      log.warn("Couldn't find descriptor property in OuterClass", e);
      return Optional.empty();
    }
  }

  private static Optional<Class> getProtobufSerializedClass(
      SerializationSchema serializationSchema) {
    Optional<Class> parameterType =
        Arrays.stream(serializationSchema.getClass().getMethods())
            .filter(m -> m.getName().equalsIgnoreCase("serialize"))
            .filter(m -> m.getParameterTypes()[0] != Object.class)
            .findAny()
            .map(m -> m.getParameterTypes()[0]);
    log.debug("Serializer serializing {}", parameterType);
    return parameterType;
  }

  private static Optional<Class> protobufParentClass() {
    try {
      return Optional.of(
          ProtobufUtils.class.getClassLoader().loadClass("com.google.protobuf.Message"));
    } catch (ClassNotFoundException e) {
      log.debug("Protobuf class not present");
      return Optional.empty();
    }
  }
}
