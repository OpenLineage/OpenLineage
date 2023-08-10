/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.wrapper;

import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.avro.AvroSerializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;

public class AvroUtils {
  public static Optional<Schema> getRegistryAvroSchema(
      Optional<SerializationSchema> serializationSchema) {
    return serializationSchema
        .filter(schema -> schema instanceof RegistryAvroSerializationSchema)
        .map(schema -> (RegistryAvroSerializationSchema) schema)
        .flatMap(
            schema -> {
              WrapperUtils.invoke(
                  RegistryAvroSerializationSchema.class, schema, "checkAvroInitialized");
              return WrapperUtils.invoke(AvroSerializationSchema.class, schema, "getDatumWriter");
            })
        .flatMap(writer -> WrapperUtils.<Schema>getFieldValue(writer.getClass(), writer, "root"));
  }

    public static Optional<Schema> getAvroSchema(Optional<SerializationSchema> serializationSchema) {
        return serializationSchema
                .filter(schema -> schema instanceof AvroSerializationSchema)
                .map(schema -> (AvroSerializationSchema) schema)
                .flatMap(
                        schema -> {
                            WrapperUtils.invoke(AvroSerializationSchema.class, schema, "checkAvroInitialized");
                            return WrapperUtils.invoke(AvroSerializationSchema.class, schema, "getDatumWriter");
                        })
                .flatMap(writer -> WrapperUtils.<Schema>getFieldValue(writer.getClass(), writer, "root"));
    }
}
