/*
/* Copyright 2018-2025 contributors to the OpenLineage project
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

/** Class used to extract type information from the facets returned by the collector */
@Slf4j
public class AvroTypeDatasetFacetVisitorDelegate {

  private final OpenLineageContext context;

  public AvroTypeDatasetFacetVisitorDelegate(OpenLineageContext context) {
    this.context = context;
  }

  public static boolean isApplicable() {
    // TODO: test this is not applicable when no avro classes are on the classpath
    return ClassUtils.hasAvroClasses();
  }

  public boolean isDefinedAt(TypeInformation typeInformation) {
    // check if this class has schema
    return typeInformation instanceof AvroTypeInfo;
  }

  Optional<SchemaDatasetFacet> delegate(TypeInformation avroTypeInfo) {
    // check if this class has schema
    Class typeClazz = avroTypeInfo.getTypeClass();
    if (SpecificRecordBase.class.isAssignableFrom(typeClazz)) {
      Schema schema = SpecificData.get().getSchema(typeClazz);
      return Optional.of(convert(context.getOpenLineage(), schema));
    } else {
      log.warn("Unsupported Avro Type: {}", typeClazz);
      return Optional.empty();
    }
  }

  // TODO: write tests to this class
}
