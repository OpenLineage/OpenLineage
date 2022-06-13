/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.execution.command.TruncateTableCommand;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.collection.Map$;
import scala.collection.immutable.HashMap;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

@ExtendWith(SparkAgentTestExtension.class)
public class TruncateTableCommandVisitorTest {

  SparkSession session;
  TruncateTableCommandVisitor visitor;
  TruncateTableCommand command;
  String database;
  TableIdentifier table = new TableIdentifier("truncate_table");

  @BeforeEach
  public void setup() {
    session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master("local")
            .getOrCreate();

    database = session.catalog().currentDatabase();
    command = new TruncateTableCommand(table, Option.empty());
    visitor = new TruncateTableCommandVisitor(SparkAgentTestExtension.newContext(session));
  }

  @AfterEach
  public void afterEach() {
    session.sessionState().catalog().dropTable(table, true, true);
  }

  @Test
  public void testTruncateTableCommandWhenTableDoesNotExist() {
    // make sure table does not exist
    session.sessionState().catalog().dropTable(table, true, true);
    try {
      command.run(session);
      // exception should be thrown before
      Assert.fail();
    } catch (Exception e) {
      // do nothing, Scala NoSuchTableException thrown
    }

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertThat(datasets).isEmpty();
  }

  @Test
  public void testTruncateCommand() {
    // create some table
    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("field1", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });
    session.catalog().createTable("truncate_table", "csv", schema, Map$.MODULE$.empty());

    // apply the visitor before running the command
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(null, datasets.get(0).getFacets().getSchema());
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/truncate_table")
        .hasFieldOrPropertyWithValue("namespace", "file");

    assertEquals(
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.TRUNCATE,
        datasets.get(0).getFacets().getLifecycleStateChange().getLifecycleStateChange());
  }
}
