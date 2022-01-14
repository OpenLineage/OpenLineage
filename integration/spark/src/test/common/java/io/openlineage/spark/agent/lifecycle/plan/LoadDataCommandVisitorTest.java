/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.execution.command.LoadDataCommand;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.Map$;
import scala.collection.immutable.HashMap;

class LoadDataCommandVisitorTest {
  @Test
  void testLoadDataCommand() {
    SparkSession session =
        SparkSession.builder()
            .config("spark.sql.warehouse.dir", "/tmp/warehouse")
            .master("local")
            .getOrCreate();
    String database = session.catalog().currentDatabase();

    StructType schema =
        new StructType(
            new StructField[] {
              new StructField("key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
              new StructField("value", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
            });

    session.catalog().createTable("table", "csv", schema, Map$.MODULE$.empty());

    LoadDataCommandVisitor visitor =
        new LoadDataCommandVisitor(SparkAgentTestExtension.newContext(session));

    LoadDataCommand command =
        new LoadDataCommand(
            TableIdentifier$.MODULE$.apply("table", Option.apply(database)),
            "/path/to/data",
            true,
            false,
            Option.empty());

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/tmp/warehouse/table")
        .hasFieldOrPropertyWithValue("namespace", "file");
  }
}
