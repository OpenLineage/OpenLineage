/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.Arrays;
import java.util.stream.Stream;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@SuppressWarnings("PMD.AvoidDuplicateLiterals")
class PlanUtilsTest {

  @ParameterizedTest
  @MethodSource()
  @SuppressWarnings("PMD.UnusedPrivateMethod")
  private static Stream<Arguments> provideJdbcUrls() {
    return Stream.of(
        Arguments.of(
            "jdbc:sqlserver://localhost;user=MyUserName;password=P%20%4021",
            "sqlserver://localhost;"),
        Arguments.of(
            "jdbc:sqlserver://localhost;USER=MyUserName;paSSword=P%20%4021",
            "sqlserver://localhost;"),
        Arguments.of(
            "jdbc:sqlserver://localhost\\instance1;databaseName=AdventureWorks;integratedSecurity=true",
            "sqlserver://localhost\\instance1;databaseName=AdventureWorks;integratedSecurity=true"),
        Arguments.of(
            "jdbc:sqlserver://;serverName=3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1;integratedSecurity=true;",
            "sqlserver://;serverName=3ffe:8311:eeee:f70f:0:5eae:10.203.31.9\\instance1;integratedSecurity=true;"),
        Arguments.of(
            "jdbc:db2://test.host.com:5021/test:user=dbadm;password=dbadm;",
            "db2://test.host.com:5021/test:"),
        Arguments.of("jdbc:db2://server:user=dbadm", "db2://server:"),
        Arguments.of(
            "jdbc:db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test",
            "db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test"),
        Arguments.of(
            "jdbc:db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test:user=dba%20adm;password=db%40dm;",
            "db2://[2001:DB8:0:0:8:800:200C:417A]:5021/test:"),
        Arguments.of(
            "jdbc:postgresql://localhost/test?user=fred&password=secret&ssl=true",
            "postgres://localhost/test"),
        Arguments.of(
            "jdbc:postgresql://localhost:5432/test?user=fred&password=secret&ssl=true",
            "postgres://localhost:5432/test"),
        Arguments.of(
            "jdbc:postgresql://192.168.1.1:5432/test?user=fred&password=secret&ssl=true",
            "postgres://192.168.1.1:5432/test"),
        Arguments.of("jdbc:postgresql://192.168.1.1:5432", "postgres://192.168.1.1:5432"),
        Arguments.of(
            "jdbc:oracle:thin:us%2Ar/p%40%24%24w0rd@some.host:1521:orcl",
            "oracle:thin:@some.host:1521:orcl"),
        Arguments.of(
            "jdbc:oracle:thin:@10.253.102.122:1521:dg01?key=value",
            "oracle:thin:@10.253.102.122:1521:dg01"),
        Arguments.of(
            "jdbc:oracle:thin:user/password@some.host:1521:orcl",
            "oracle:thin:@some.host:1521:orcl"),
        Arguments.of(
            "jdbc:oracle:thin:user/password@//localhost:1521/serviceName",
            "oracle:thin:@//localhost:1521/serviceName"),
        Arguments.of(
            "jdbc:oracle:thin:@(description=(address=(protocol=tcp)(port=1521)(host=prodHost)))(connect_data=(INSTANCE_NAME=ORCL)))",
            "oracle:thin:@(description=(address=(protocol=tcp)(port=1521)(host=prodHost)))(connect_data=(INSTANCE_NAME=ORCL)))"),
        Arguments.of(
            "jdbc:oracle:thin:@(DESCRIPTION= (LOAD_BALANCE=on) (ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1) (PORT=1521)) (ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=5221))) (CONNECT_DATA=(SERVICE_NAME=orcl)))",
            "oracle:thin:@(DESCRIPTION= (LOAD_BALANCE=on) (ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=host1) (PORT=1521)) (ADDRESS=(PROTOCOL=TCP)(HOST=host2)(PORT=5221))) (CONNECT_DATA=(SERVICE_NAME=orcl)))"),
        Arguments.of("jdbc:oracle:thin:@localhost:1521:orcl", "oracle:thin:@localhost:1521:orcl"),
        Arguments.of(
            "jdbc:oracle:thin:@//localhost:1521/serviceName",
            "oracle:thin:@//localhost:1521/serviceName"),
        Arguments.of(
            "jdbc:oracle:thin:@dbname_high?TNS_ADMIN=/Users/test/wallet_dbname",
            "oracle:thin:@dbname_high"),
        Arguments.of(
            "jdbc:mysql://10.253.102.122:1521/dbname;key=value",
            "mysql://10.253.102.122:1521/dbname;key=value"),
        Arguments.of(
            "jdbc:mysql://10.253.102.122:1521/dbname;user=user;password=pwd;key=value",
            "mysql://10.253.102.122:1521/dbname;key=value"),
        Arguments.of("jdbc:mysql://host.name.com/dbname", "mysql://host.name.com/dbname"),
        Arguments.of(
            "jdbc:mysql://username:pwd@host.name.com/dbname", "mysql://host.name.com/dbname"),
        Arguments.of(
            "jdbc:mysql://username:pwd@host.name.com/dbname?key=value;key2=value2",
            "mysql://host.name.com/dbname"),
        Arguments.of(
            "jdbc:mysql://username:pwd@myhost1:1111,username:pwd@myhost2:2222/db",
            "mysql://myhost1:1111,myhost2:2222/db"),
        Arguments.of(
            "jdbc:mysql://address=(host=myhost1)(port=1111)(user=sandy)(password=secret),address=(host=myhost2)(port=2222)(user=finn)(password=secret)/db",
            "mysql://address=(host=myhost1)(port=1111),address=(host=myhost2)(port=2222)/db"));
  }

  @Test
  void testSchemaFacetFlat() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);
    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$);

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(2);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testSchemaFacetNested() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    StructType superNested = new StructType().add("d", IntegerType$.MODULE$);

    StructType nested =
        new StructType()
            .add("c", StringType$.MODULE$, false, "c description")
            .add("super_nested", superNested);

    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$)
            .add("nested", nested, true, "nested description");

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(3);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields nestedField = schemaDatasetFacet.getFields().get(2);
    assertThat(nestedField)
        .hasFieldOrPropertyWithValue("name", "nested")
        .hasFieldOrPropertyWithValue("type", "struct")
        .hasFieldOrPropertyWithValue("description", "nested description");

    assertThat(nestedField.getFields()).hasSize(2);

    assertThat(nestedField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "c")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "c description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(nestedField.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "super_nested")
        .hasFieldOrPropertyWithValue("type", "struct")
        .hasFieldOrPropertyWithValue("description", null);

    OpenLineage.SchemaDatasetFacetFields superNestedField = nestedField.getFields().get(1);
    assertThat(superNestedField.getFields()).hasSize(1);
    assertThat(superNestedField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "d")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testSchemaFacetMap() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    MapType map = new MapType(StringType$.MODULE$, IntegerType$.MODULE$, false);
    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$)
            .add("nested", map, true, "nested description");

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(3);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields mapField = schemaDatasetFacet.getFields().get(2);
    assertThat(mapField)
        .hasFieldOrPropertyWithValue("name", "nested")
        .hasFieldOrPropertyWithValue("type", "map")
        .hasFieldOrPropertyWithValue("description", "nested description");

    assertThat(mapField.getFields()).hasSize(2);

    assertThat(mapField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "key")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(mapField.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "value")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testSchemaFacetArray() {
    OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

    StructType nested = new StructType().add("c", StringType$.MODULE$, false, "c description");
    ArrayType arrayNested = new ArrayType(nested, true);

    ArrayType arrayPrimitive = new ArrayType(StringType$.MODULE$, false);
    StructType structType =
        new StructType()
            .add("a", StringType$.MODULE$, false, "a description")
            .add("b", IntegerType$.MODULE$)
            .add("arrayPrimitive", arrayPrimitive, true, "arrayPrimitive description")
            .add("arrayNested", arrayNested, false);

    OpenLineage.SchemaDatasetFacet schemaDatasetFacet =
        PlanUtils.schemaFacet(openLineage, structType);

    assertThat(schemaDatasetFacet.getFields()).hasSize(4);

    assertThat(schemaDatasetFacet.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "a description")
        .hasFieldOrPropertyWithValue("fields", null);

    assertThat(schemaDatasetFacet.getFields().get(1))
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("type", "integer")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields arrayPrimitiveField =
        schemaDatasetFacet.getFields().get(2);
    assertThat(arrayPrimitiveField)
        .hasFieldOrPropertyWithValue("name", "arrayPrimitive")
        .hasFieldOrPropertyWithValue("type", "array")
        .hasFieldOrPropertyWithValue("description", "arrayPrimitive description");

    assertThat(arrayPrimitiveField.getFields()).hasSize(1);
    assertThat(arrayPrimitiveField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", null)
        .hasFieldOrPropertyWithValue("fields", null);

    OpenLineage.SchemaDatasetFacetFields arrayNestedField = schemaDatasetFacet.getFields().get(3);
    assertThat(arrayNestedField.getFields()).hasSize(1);
    assertThat(arrayNestedField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "_element")
        .hasFieldOrPropertyWithValue("type", "struct")
        .hasFieldOrPropertyWithValue("description", null);

    OpenLineage.SchemaDatasetFacetFields arrayNestedElementField =
        arrayNestedField.getFields().get(0);
    assertThat(arrayNestedElementField.getFields()).hasSize(1);
    assertThat(arrayNestedElementField.getFields().get(0))
        .hasFieldOrPropertyWithValue("name", "c")
        .hasFieldOrPropertyWithValue("type", "string")
        .hasFieldOrPropertyWithValue("description", "c description")
        .hasFieldOrPropertyWithValue("fields", null);
  }

  @Test
  void testListOfAttributesToStructType() {
    Attribute attribute1 = mock(Attribute.class);
    when(attribute1.name()).thenReturn("a");
    when(attribute1.dataType()).thenReturn(StringType$.MODULE$);

    Attribute attribute2 = mock(Attribute.class);
    when(attribute2.name()).thenReturn("b");
    when(attribute2.dataType()).thenReturn(IntegerType$.MODULE$);

    StructType schema = PlanUtils.toStructType(Arrays.asList(attribute1, attribute2));

    assertThat(schema.fields()).hasSize(2);
    assertThat(schema.fields()[0])
        .hasFieldOrPropertyWithValue("name", "a")
        .hasFieldOrPropertyWithValue("dataType", StringType$.MODULE$)
        .hasFieldOrPropertyWithValue("nullable", false)
        .hasFieldOrPropertyWithValue("metadata", null);
    assertThat(schema.fields()[1])
        .hasFieldOrPropertyWithValue("name", "b")
        .hasFieldOrPropertyWithValue("dataType", IntegerType$.MODULE$)
        .hasFieldOrPropertyWithValue("nullable", false)
        .hasFieldOrPropertyWithValue("metadata", null);
  }
}
