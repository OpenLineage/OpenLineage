package io.openlineage.spark.agent;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration-test")
@Slf4j
@Disabled
class RddUnionIntegrationTest {
  private static ObjectMapper mapper;
  private static Path tempDir;
  private static Path warehouseDir;
  private static Path testResultsDir;

  @BeforeAll
  @SneakyThrows
  static void beforeAll() {
    mapper = new ObjectMapper().findAndRegisterModules();
    tempDir = Files.createTempDirectory(RddUnionIntegrationTest.class.getSimpleName());
    warehouseDir = tempDir.resolve("warehouse");
    testResultsDir = tempDir.resolve("test-results");

    log.info(
        "Running RddUnionIntegrationTest with the following configuration:\n    tempDir: {}\n    warehouseDir: {}\n    testResultsDir: {}\n",
        tempDir,
        warehouseDir,
        testResultsDir);
  }

  @AfterAll
  static void afterAll() {
    tempDir.toFile().deleteOnExit();
  }

  @Test
  @SneakyThrows
  void testRddUnion() {
    String testFileName = "test-rdd-union.json";
    Path testFilePath = testResultsDir.resolve(testFileName);

    SparkSession spark =
        SparkSession.builder()
            .appName("RddUnionIntegrationTest")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", false)
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.sql.warehouse.dir", warehouseDir.toString())
            .config("spark.openlineage.transport.type", "file")
            .config("spark.openlineage.transport.location", testFilePath.toString())
            .config("spark.openlineage.debugFacet", "enabled")
            .config("spark.extraListeners", OpenLineageSparkListener.class.getCanonicalName())
            .master("local")
            .getOrCreate();

    try (JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext())) {
      JavaRDD<Integer> firstJavaRdd = jsc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
      Dataset<Integer> firstDataset = spark.createDataset(firstJavaRdd.rdd(), Encoders.INT());
      firstDataset
          .repartition(1)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(warehouseDir.resolve("first_rdd").toString());

      JavaRDD<Integer> secondJavaRdd = jsc.parallelize(Arrays.asList(6, 7, 8, 9, 10));
      Dataset<Integer> secondDataset = spark.createDataset(secondJavaRdd.rdd(), Encoders.INT());
      secondDataset
          .repartition(1)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(warehouseDir.resolve("second_rdd").toString());

      Dataset<Row> firstRead = spark.read().parquet(warehouseDir.resolve("first_rdd").toString());
      Dataset<Row> secondRead = spark.read().parquet(warehouseDir.resolve("second_rdd").toString());
      Dataset<Row> union = firstRead.union(secondRead);

      union
          .repartition(1)
          .write()
          .mode(SaveMode.Overwrite)
          .parquet(warehouseDir.resolve("union_rdd").toString());
    } finally {
      spark.stop();
    }

    List<String> content = Files.readAllLines(testFilePath);
    String lastEventJson = content.get(content.size() - 1);
    RunEvent lastEvent = mapper.readValue(lastEventJson, RunEvent.class);
    assertThat(lastEvent.getOutputs().get(0))
        .hasFieldOrPropertyWithValue("namespace", "file")
        .hasFieldOrPropertyWithValue("name", warehouseDir.resolve("union_rdd").toString());

    assertThat(lastEvent.getInputs().stream().map(InputDataset::getName))
        .contains(
            warehouseDir.resolve("first_rdd").toString(),
            warehouseDir.resolve("second_rdd").toString());
  }
}
