/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.lifecycle.plan.CreateDataSourceTableAsSelectCommandVisitor;
import java.net.URI;
import java.util.List;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat$;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.execution.command.CreateDataSourceTableAsSelectCommand;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.Option;
import scala.collection.Map$;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

class CreateDataSourceTableAsSelectCommandVisitorTest {

  SparkSession session = mock(SparkSession.class);

  @BeforeEach
  public void setUp() {
    when(session.sparkContext()).thenReturn(mock(SparkContext.class));
  }

  @Test
  void testCTASCommand() {
    CreateDataSourceTableAsSelectCommandVisitor visitor =
        new CreateDataSourceTableAsSelectCommandVisitor(
            SparkAgentTestExtension.newContext(session));

    CreateDataSourceTableAsSelectCommand command =
        new CreateDataSourceTableAsSelectCommand(
            SparkUtils.catalogTable(
                TableIdentifier$.MODULE$.apply("tablename", Option.apply("db")),
                CatalogTableType.EXTERNAL(),
                CatalogStorageFormat$.MODULE$.apply(
                    Option.apply(URI.create("s3://bucket/directory")),
                    null,
                    null,
                    null,
                    false,
                    Map$.MODULE$.empty()),
                new StructType(
                    new StructField[] {
                      new StructField(
                          "key", IntegerType$.MODULE$, false, new Metadata(new HashMap<>())),
                      new StructField(
                          "value", StringType$.MODULE$, false, new Metadata(new HashMap<>()))
                    })),
            null,
            null,
            Seq$.MODULE$.<String>empty());

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(1, datasets.size());
    OpenLineage.OutputDataset outputDataset = datasets.get(0);

    assertEquals(
        OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.CREATE,
        outputDataset.getFacets().getLifecycleStateChange().getLifecycleStateChange());
    assertEquals("directory", outputDataset.getName());
    assertEquals("s3://bucket", outputDataset.getNamespace());
  }
}
