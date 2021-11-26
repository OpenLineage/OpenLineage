package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.AlterTableAddColumnsCommand;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Map$;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
public class AlterTableAddColumnsCommandVisitorTest {

  SparkSession session;
  AlterTableAddColumnsCommandVisitor visitor;
  String database;

  @AfterEach
  public void afterEach() {
    dropTables();
  }

  private void dropTables() {
    session
        .sessionState()
        .catalog()
        .dropTable(new TableIdentifier("table1", Option.apply(database)), true, true);
  }

  @BeforeEach
  public void setup() {
    session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master("local")
            .getOrCreate();

    dropTables();

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("col1", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    session.catalog().createTable("table1", "csv", schema, Map$.MODULE$.empty());
    database = session.catalog().currentDatabase();
    visitor = new AlterTableAddColumnsCommandVisitor(session);
  }

  @Test
  public void testAlterTableAddColumns() {
    AlterTableAddColumnsCommand command =
        new AlterTableAddColumnsCommand(
            new TableIdentifier("table1", Option.apply(database)),
            JavaConversions.asScalaIterator(
                    Arrays.asList(
                            new StructField(
                                "col2", StringType$.MODULE$, false, new Metadata(new HashMap<>())),
                            new StructField(
                                "col3", StringType$.MODULE$, false, new Metadata(new HashMap<>())))
                        .iterator())
                .toSeq());

    command.run(session);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.Dataset> datasets = visitor.apply(command);
    assertEquals(3, datasets.get(0).getFacets().getSchema().getFields().size());
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/table1")
        .hasFieldOrPropertyWithValue("namespace", "file");
  }

  @Test
  public void testAlterUpdateColumnsBeforeCommandRun() {
    AlterTableAddColumnsCommand command =
        new AlterTableAddColumnsCommand(
            new TableIdentifier("table1", Option.apply(database)),
            JavaConversions.asScalaIterator(
                    Arrays.asList(
                            new StructField(
                                "col2", StringType$.MODULE$, false, new Metadata(new HashMap<>())))
                        .iterator())
                .toSeq());

    // command is not run
    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.Dataset> datasets = visitor.apply(command);
    assertThat(datasets).isEmpty();
  }
}
