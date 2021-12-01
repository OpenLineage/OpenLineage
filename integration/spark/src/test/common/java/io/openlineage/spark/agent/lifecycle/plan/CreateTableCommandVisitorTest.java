package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.lifecycle.CatalogTableTestUtils;
import java.util.List;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.CreateTableCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;

@ExtendWith(SparkAgentTestExtension.class)
public class CreateTableCommandVisitorTest {

  CreateTableCommandVisitor visitor;
  String database = "default";
  CreateTableCommand command;
  TableIdentifier table = new TableIdentifier("create_table", Option.apply(database));

  @BeforeEach
  public void setup() {
    command = new CreateTableCommand(CatalogTableTestUtils.getCatalogTable(table), true);
    visitor = new CreateTableCommandVisitor();
  }

  @Test
  void testCreateTableCommand() {
    List<OpenLineage.Dataset> datasets = visitor.apply(command);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    assertEquals(1, datasets.get(0).getFacets().getSchema().getFields().size());
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/some-location")
        .hasFieldOrPropertyWithValue("namespace", "file");
  }
}
