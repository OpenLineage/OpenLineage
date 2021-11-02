package io.openlineage.spark.agent.lifecycle.plan;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.SparkAgentTestExtension;
import java.net.URI;
import java.util.List;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.TableIdentifier$;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat;
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat$;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTable$;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.execution.command.CreateDataSourceTableCommand;
import org.apache.spark.sql.types.IntegerType$;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StringType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import scala.Option;
import scala.collection.Map$;
import scala.collection.Seq$;
import scala.collection.immutable.HashMap;

@ExtendWith(SparkAgentTestExtension.class)
class CreateDataSourceTableCommandVisitorTest {

  @Test
  void testCreateDataSourceTableCommand() {
    CreateDataSourceTableCommandVisitor visitor =
        new CreateDataSourceTableCommandVisitor();

    CreateDataSourceTableCommand command =
        new CreateDataSourceTableCommand(
            table(
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
            false);

    assertThat(visitor.isDefinedAt(command)).isTrue();
    List<OpenLineage.Dataset> datasets = visitor.apply(command);
    assertThat(datasets)
        .singleElement()
        .hasFieldOrPropertyWithValue("name", "/directory")
        .hasFieldOrPropertyWithValue("namespace", "s3://bucket");
  }

  // Can't use Scala's default parameters from Java.
  CatalogTable table(
      TableIdentifier identifier,
      CatalogTableType tableType,
      CatalogStorageFormat storageFormat,
      StructType schema) {
    return CatalogTable$.MODULE$.apply(
        identifier,
        tableType,
        storageFormat,
        schema,
        null,
        Seq$.MODULE$.<String>empty(),
        null,
        "",
        System.currentTimeMillis(),
        -1L,
        "",
        Map$.MODULE$.empty(),
        null,
        null,
        null,
        Seq$.MODULE$.<String>empty(),
        false,
        true,
        Map$.MODULE$.empty());
  }
}
