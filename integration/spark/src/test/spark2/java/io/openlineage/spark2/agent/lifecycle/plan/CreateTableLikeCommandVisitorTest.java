/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark2.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.CREATE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.execution.command.CreateTableLikeCommand;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.collection.Map$;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
class CreateTableLikeCommandVisitorTest {
  @Test
  void testCreateTableLikeCommand() {
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

    CreateTableLikeCommandVisitor visitor =
        new CreateTableLikeCommandVisitor(SparkAgentTestExtension.newContext(session));

    CreateTableLikeCommand command =
        new CreateTableLikeCommand(
            TableIdentifier$.MODULE$.apply("table", Option.apply(database)),
            TableIdentifier$.MODULE$.apply("table", Option.apply(database)),
            Option.apply("/path/to/data"),
            false);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(1, datasets.size());
    OpenLineage.OutputDataset outputDataset = datasets.get(0);

    assertEquals(
        new TableStateChangeFacet(CREATE),
        outputDataset.getFacets().getAdditionalProperties().get("tableStateChange"));
    assertEquals("/path/to/data", outputDataset.getName());
    assertEquals("file", outputDataset.getNamespace());
  }
}
