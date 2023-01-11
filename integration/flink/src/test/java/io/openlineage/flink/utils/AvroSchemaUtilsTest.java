/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import io.openlineage.client.OpenLineage;
import java.net.URI;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

class AvroSchemaUtilsTest {

  Schema schema =
      SchemaBuilder.record("InputEvent")
          .namespace("io.openlineage.flink.avro.event")
          .fields()
          .name("a")
          .doc("some-doc")
          .type()
          .nullable()
          .longType()
          .noDefault()
          .name("b")
          .type()
          .nullable()
          .intType()
          .noDefault()
          .name("c")
          .type()
          .nullable()
          .stringType()
          .stringDefault("")
          .endRecord();

  OpenLineage openLineage = new OpenLineage(mock(URI.class));

  @Test
  void testConvert() {
    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        AvroSchemaUtils.convert(openLineage, schema);

    List<OpenLineage.SchemaDatasetFacetFields> fields = schemaDatasetFacet.getFields();

    assertEquals(3, fields.size());

    assertEquals("a", fields.get(0).getName());
    assertEquals("some-doc", fields.get(0).getDescription());

    assertEquals("long", fields.get(0).getType());
    assertEquals("int", fields.get(1).getType());
    assertEquals("string", fields.get(2).getType());
  }
}
