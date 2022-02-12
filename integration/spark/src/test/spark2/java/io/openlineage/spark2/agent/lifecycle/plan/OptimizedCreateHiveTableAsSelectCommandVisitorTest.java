/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark2.agent.lifecycle.plan;

import static io.openlineage.spark.agent.facets.TableStateChangeFacet.StateChange.OVERWRITE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import io.openlineage.spark.agent.facets.TableStateChangeFacet;
import io.openlineage.spark.agent.lifecycle.plan.OptimizedCreateHiveTableAsSelectCommandVisitor;
import io.openlineage.spark.agent.util.ScalaConversionUtils;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.Partition;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat$;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.expressions.AttributeReference;
import org.apache.spark.sql.catalyst.expressions.ExprId;
import org.apache.spark.sql.execution.datasources.LogicalRelation;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions;
import org.apache.spark.sql.execution.datasources.jdbc.JDBCRelation;
import org.apache.spark.sql.hive.execution.OptimizedCreateHiveTableAsSelectCommand;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.Driver;
import scala.Option;
import scala.Tuple2;
import scala.collection.Map$;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
class OptimizedCreateHiveTableAsSelectCommandVisitorTest {

  @Test
  void testOptimizedCreateHiveTableAsSelectCommand() {
    SparkSession session = SparkSession.builder().master("local").getOrCreate();
    OptimizedCreateHiveTableAsSelectCommandVisitor visitor =
        new OptimizedCreateHiveTableAsSelectCommandVisitor(
            SparkAgentTestExtension.newContext(session));

    OptimizedCreateHiveTableAsSelectCommand command =
        new OptimizedCreateHiveTableAsSelectCommand(
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
            new LogicalRelation(
                new JDBCRelation(
                    new StructType(
                        new StructField[] {
                          new StructField("key", IntegerType$.MODULE$, false, null),
                          new StructField("value", StringType$.MODULE$, false, null)
                        }),
                    new Partition[] {},
                    new JDBCOptions(
                        "",
                        "temp",
                        scala.collection.immutable.Map$.MODULE$
                            .newBuilder()
                            .$plus$eq(Tuple2.apply("driver", Driver.class.getName()))
                            .result()),
                    session),
                Seq$.MODULE$
                    .<AttributeReference>newBuilder()
                    .$plus$eq(
                        new AttributeReference(
                            "key",
                            IntegerType$.MODULE$,
                            false,
                            null,
                            ExprId.apply(1L),
                            Seq$.MODULE$.<String>empty()))
                    .$plus$eq(
                        new AttributeReference(
                            "value",
                            StringType$.MODULE$,
                            false,
                            null,
                            ExprId.apply(2L),
                            Seq$.MODULE$.<String>empty()))
                    .result(),
                Option.empty(),
                false),
            ScalaConversionUtils.fromList(Arrays.asList("key", "value")),
            SaveMode.Overwrite);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.OutputDataset> datasets = visitor.apply(command);

    assertEquals(1, datasets.size());
    OpenLineage.OutputDataset outputDataset = datasets.get(0);

    assertEquals(
        new TableStateChangeFacet(OVERWRITE),
        outputDataset.getFacets().getAdditionalProperties().get("tableStateChange"));
    assertEquals("directory", outputDataset.getName());
    assertEquals("s3://bucket", outputDataset.getNamespace());
  }
}
