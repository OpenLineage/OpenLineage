/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.visitor.facet;

import static io.openlineage.flink.utils.AvroSchemaUtils.convert;

import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.flink.api.OpenLineageContext;
import io.openlineage.flink.utils.ClassUtils;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;

/** Class used to extract type information from the facets returned by the collector */
@Slf4j
public class AvroTypeDatasetFacetVisitorDelegate {

  private final OpenLineageContext context;

  public AvroTypeDatasetFacetVisitorDelegate(OpenLineageContext context) {
    this.context = context;
  }

  public static boolean isApplicable() {
    return ClassUtils.hasAvroClasses();
  }

  public boolean isDefinedAt(TypeInformation typeInformation) {
    // check if this class has schema
    return typeInformation instanceof AvroTypeInfo
        || typeInformation instanceof GenericRecordAvroTypeInfo;
  }

  Optional<SchemaDatasetFacet> delegate(TypeInformation avroTypeInfo) {
    // check if this class has schema
    if (avroTypeInfo instanceof GenericRecordAvroTypeInfo) {
      return genericRecordSchema((GenericRecordAvroTypeInfo) avroTypeInfo)
          .map(schema -> convert(context.getOpenLineage(), schema));
    }
    Class typeClazz = avroTypeInfo.getTypeClass();
    if (SpecificRecordBase.class.isAssignableFrom(typeClazz)) {
      Schema schema = SpecificData.get().getSchema(typeClazz);
      return Optional.of(convert(context.getOpenLineage(), schema));
    } else {
      log.warn("Unsupported Avro Type: {}", typeClazz);
      return Optional.empty();
    }
  }

  /**
   * {@link GenericRecordAvroTypeInfo} carries the Avro schema in a private field without an
   * accessor, so it is read reflectively. This covers records deserialized from schema registries
   * (e.g. Confluent Schema Registry, AWS Glue Schema Registry) as {@code GenericRecord}.
   */
  private Optional<Schema> genericRecordSchema(GenericRecordAvroTypeInfo typeInfo) {
    try {
      java.lang.reflect.Field schemaField =
          GenericRecordAvroTypeInfo.class.getDeclaredField("schema");
      schemaField.setAccessible(true);
      return Optional.ofNullable((Schema) schemaField.get(typeInfo));
    } catch (ReflectiveOperationException | SecurityException e) {
      log.warn("Could not extract Avro schema from GenericRecordAvroTypeInfo", e);
      return Optional.empty();
    }
  }
}
