/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import com.google.protobuf.Descriptors.FileDescriptor;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.SchemaDatasetFacetFields;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.flink.api.common.serialization.DeserializationSchema;
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
    Optional<Class> messageClass = protobufMessageClass();
    if (messageClass.isEmpty()) {
      log.debug("Protobuf parent Message class not loaded");
      return false;
    }

    return getProtobufSerializeClass(serializationSchema)
        .map(t -> messageClass.get().isAssignableFrom(t))
        .orElse(false);
  }

  /**
   * Given a deserialization schema, the method verifies if a produced output type extends Protobuf
   * generated classes.
   *
   * @param deserializationSchema
   * @return
   */
  public static boolean isProtobufDeserializationSchema(
      DeserializationSchema deserializationSchema) {
    Optional<Class> messageClass = protobufMessageClass();
    if (messageClass.isEmpty()) {
      log.debug("Protobuf parent Message class not loaded");
      return false;
    }

    return getProtobufDeserializeClass(deserializationSchema)
        .map(t -> messageClass.get().isAssignableFrom(t))
        .orElse(false);
  }

  public static Optional<OpenLineage.SchemaDatasetFacet> convert(
      OpenLineage openLineage, SerializationSchema serializationSchema) {
    Optional<Class> protobufSerializedClass = getProtobufSerializeClass(serializationSchema);

    if (protobufSerializedClass.isEmpty()) {
      return Optional.empty();
    }

    List<SchemaDatasetFacetFields> fields =
        getSchemaDatasetFacetFields(openLineage, protobufSerializedClass.get());
    if (!fields.isEmpty()) {
      OpenLineage.SchemaDatasetFacetBuilder builder = openLineage.newSchemaDatasetFacetBuilder();
      return Optional.of(builder.fields(fields).build());
    }

    return Optional.empty();
  }

  public static Optional<OpenLineage.SchemaDatasetFacet> convert(
      OpenLineage openLineage, DeserializationSchema deserializationSchema) {
    Optional<Class> protobufSerializedClass = getProtobufDeserializeClass(deserializationSchema);

    if (protobufSerializedClass.isEmpty()) {
      return Optional.empty();
    }

    List<SchemaDatasetFacetFields> fields =
        getSchemaDatasetFacetFields(openLineage, protobufSerializedClass.get());
    if (!fields.isEmpty()) {
      OpenLineage.SchemaDatasetFacetBuilder builder = openLineage.newSchemaDatasetFacetBuilder();
      return Optional.of(builder.fields(fields).build());
    }

    return Optional.empty();
  }

  static List<SchemaDatasetFacetFields> getSchemaDatasetFacetFields(
      OpenLineage openLineage, Class protobufSerializedClass) {
    try {
      Class<?> staticDefintionClass =
          Class.forName(protobufSerializedClass.getCanonicalName() + "OuterClass");

      Field descriptorField = FieldUtils.getField(staticDefintionClass, "descriptor", true);
      FileDescriptor fileDescriptor = (FileDescriptor) descriptorField.get(staticDefintionClass);

      ProtobufFieldResolver fieldResolver = new ProtobufFieldResolver(openLineage);
      return fileDescriptor.getMessageTypes().stream()
          .flatMap(d -> d.getFields().stream())
          .filter(d -> protobufSerializedClass.getName().contains(d.getContainingType().getName()))
          .map(f -> fieldResolver.resolveField(f))
          .collect(Collectors.toList());
    } catch (ClassNotFoundException e) {
      // swallow it
      log.warn("Couldn't find OuterClass for {}: {}", protobufSerializedClass, e);
      return Collections.emptyList();
    } catch (IllegalAccessException e) {
      // swallow it
      log.warn("Couldn't find descriptor property in OuterClass", e);
      return Collections.emptyList();
    }
  }

  /**
   * Given a serialization schema class, this method finds its serialize method which takes
   * parameter different from {@link Object}. Having proper serialize method, the code identifies
   * first parameter of the serialize method, which is the protobuf class we are looking for.
   *
   * @param serializationSchema
   * @return
   */
  private static Optional<Class> getProtobufSerializeClass(
      SerializationSchema serializationSchema) {
    Optional<Class> parameterType =
        Arrays.stream(serializationSchema.getClass().getMethods())
            .filter(m -> "serialize".equalsIgnoreCase(m.getName()))
            .filter(m -> m.getParameterTypes()[0] != Object.class)
            .findAny()
            .map(m -> m.getParameterTypes()[0]);
    log.debug("Serializer serializing {}", parameterType);
    return parameterType;
  }

  /**
   * Given a deserialization schema class, this method finds it deserialize method whose return type
   * is different from {@link Object}. The return type of deserialize method is protobuf class we
   * are looking for.
   *
   * @param deserializationSchema
   * @return
   */
  private static Optional<Class> getProtobufDeserializeClass(
      DeserializationSchema deserializationSchema) {
    if (deserializationSchema.getProducedType() != null) {
      return Optional.of(deserializationSchema.getProducedType().getTypeClass());
    } else {
      Optional<Class> parameterType =
          Arrays.stream(deserializationSchema.getClass().getMethods())
              .filter(m -> "deserialize".equalsIgnoreCase(m.getName()))
              .filter(m -> m.getReturnType() != Object.class)
              .findAny()
              .map(m -> m.getReturnType());
      log.debug("Deserializer serializing {}", parameterType);
      return parameterType;
    }
  }

  private static Optional<Class> protobufMessageClass() {
    try {
      return Optional.of(
          ProtobufUtils.class.getClassLoader().loadClass("com.google.protobuf.Message"));
    } catch (ClassNotFoundException e) {
      log.debug("Protobuf class not present");
      return Optional.empty();
    }
  }
}
