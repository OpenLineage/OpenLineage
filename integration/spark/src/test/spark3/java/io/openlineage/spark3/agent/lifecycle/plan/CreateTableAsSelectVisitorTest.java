package io.openlineage.spark3.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.util.List;
import org.apache.commons.lang.reflect.FieldUtils;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.CreateTableAsSelect;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation$;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import scala.collection.Map$;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
class CreateTableAsSelectVisitorTest {

  private CreateTableAsSelect createCTAS(TableCatalog tableCatalog) {
    return new CreateTableAsSelect(
        tableCatalog,
        new Identifier() {
          @Override
          public String[] namespace() {
            return new String[] {"database", "schema"};
          }

          @Override
          public String name() {
            return "table";
          }

          @Override
          public String toString() {
            return "database.schema.table";
          }
        },
        Seq$.MODULE$.empty(),
        LocalRelation$.MODULE$.apply(
            new StructField("key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
            Seq$.MODULE$.<StructField>empty()),
        Map$.MODULE$.empty(),
        Map$.MODULE$.empty(),
        false);
  }

  @Test
  void testCreateTableAsSelectJdbcCommand() throws IllegalAccessException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    CreateTableAsSelectVisitor visitor = new CreateTableAsSelectVisitor(session);

    JDBCTableCatalog tableCatalog = new JDBCTableCatalog();
    JDBCOptions options = mock(JDBCOptions.class);
    when(options.url()).thenReturn("jdbc:postgresql://postgreshost:5432");
    FieldUtils.writeField(tableCatalog, "options", options, true);

    CreateTableAsSelect command = createCTAS(tableCatalog);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.Dataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "database.schema.table")
        .hasFieldOrPropertyWithValue("namespace", "postgresql://postgreshost:5432");
  }

  @ParameterizedTest
  @CsvSource({
    "hdfs://namenode:8020/warehouse,hdfs://namenode:8020,/warehouse/database.schema.table",
    "/tmp/warehouse,file,/tmp/warehouse/database.schema.table"
  })
  void testCreateTableAsSelectIcebergHadoopCommand(
      String warehouseConf, String namespace, String name) throws IllegalAccessException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    session.conf().set("spark.sql.catalog.test.type", "hadoop");
    session.conf().set("spark.sql.catalog.test.warehouse", warehouseConf);

    CreateTableAsSelectVisitor visitor = new CreateTableAsSelectVisitor(session);
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    CreateTableAsSelect command = createCTAS(sparkCatalog);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.Dataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", name)
        .hasFieldOrPropertyWithValue("namespace", namespace);
  }

  @Test
  void testCreateTableAsSelectIcebergHiveCommand() throws IllegalAccessException {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    session.conf().set("spark.sql.catalog.test.type", "hive");
    session.conf().set("spark.sql.catalog.test.uri", "thrift://metastore-host:10001");

    CreateTableAsSelectVisitor visitor = new CreateTableAsSelectVisitor(session);
    SparkCatalog sparkCatalog = mock(SparkCatalog.class);
    when(sparkCatalog.name()).thenReturn("test");

    CreateTableAsSelect command = createCTAS(sparkCatalog);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.Dataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "database.schema.table")
        .hasFieldOrPropertyWithValue("namespace", "hive://metastore-host:10001");
  }
}
