/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.app.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.app.agent.SparkAgentTestExtension;
import io.openlineage.spark.shared.agent.lifecycle.plan.DropTableCommandVisitor;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.DropTableCommand;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.collection.Map$;
import scala.collection.immutable.HashMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@ExtendWith(SparkAgentTestExtension.class)
public class DropTableCommandVisitorTest {

  SparkSession session;
  DropTableCommandVisitor visitor;
  DropTableCommand command;
  String database;
  TableIdentifier table = new TableIdentifier("drop_table");

  @BeforeEach
  public void setup() {
    session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master("local")
            .getOrCreate();

    database = session.catalog().currentDatabase();
    command = new DropTableCommand(table, true, false, true);
    visitor = new DropTableCommandVisitor(SparkAgentTestExtension.newContext(session));
  }

  @AfterEach
  public void afterEach() {
    session.sessionState().catalog().dropTable(table, true, true);
  }

  @Test
  public void testDropTableCommandWhenTableDoesNotExist() {
    // make sure table does not exist
    session.sessionState().catalog().dropTable(table, true, true);
    command.run(session);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertThat(datasets).isEmpty();
  }

  @Test
  public void testDropCommand() {
    // create some other table first
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("field1", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    session.catalog().createTable("drop_table", "csv", schema, Map$.MODULE$.empty());

    // apply the visitor before running the command
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(null, datasets.get(0).getFacets().getSchema());
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/drop_table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertThat(
        datasets
            .get(0)
            .getFacets()
            .getLifecycleStateChange()
            .getLifecycleStateChange()
            .equals(OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.DROP));
  }
}
