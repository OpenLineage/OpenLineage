package io.openlineage.spark.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.lifecycle.CatalogTableTestUtils;
import java.util.List;
import org.apache.spark.sql.SparkSession;
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
  public void setup(SparkSession sparkSession) {
    command = new CreateTableCommand(CatalogTableTestUtils.getCatalogTable(table), true);
    visitor = new CreateTableCommandVisitor(SparkAgentTestExtension.newContext(sparkSession));
  }

  @Test
  void testCreateTableCommand() {
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    OpenLineage.OutputDataset outputDataset = datasets.get(0);
    assertEquals(1, outputDataset.getFacets().getSchema().getFields().size());

    assertEquals(
        new TableStateChangeFacet(CREATE),
        outputDataset.getFacets().getAdditionalProperties().get("tableStateChange"));
    assertEquals("/some-location", outputDataset.getName());
    assertEquals("file", outputDataset.getNamespace());
  }
}
