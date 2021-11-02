package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import java.util.List;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation$;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import scala.collection.Map$;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

class CreateTableAsSelectVisitorTest {

  @Test
  void testCreateTableAsSelectCommand() throws IllegalAccessException {
    CreateTableAsSelectVisitor visitor = new CreateTableAsSelectVisitor();

    JDBCTableCatalog tableCatalog = new JDBCTableCatalog();
    JDBCOptions options = mock(JDBCOptions.class);
    when(options.url()).thenReturn("jdbc:postgresql://postgreshost:5432");
    FieldUtils.writeField(tableCatalog, "options", options, true);

    CreateTableAsSelect command =
        new CreateTableAsSelect(
            tableCatalog,
            new Identifier() {
              @Override
              public String[] namespace() {
                return new String[] {};
              }

              @Override
              public String name() {
                return "table";
              }

              @Override
              public String toString() {
                return "table";
              }
            },
            Seq$.MODULE$.empty(),
            LocalRelation$.MODULE$.apply(
                new StructField("key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
                Seq$.MODULE$.<StructField>empty()),
            Map$.MODULE$.empty(),
            Map$.MODULE$.empty(),
            false);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.Dataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "table")
        .hasFieldOrPropertyWithValue("namespace", "postgresql://postgreshost:5432");
  }
}
