package openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import openlineage.spark.agent.client.OpenLineageClient;
import openlineage.spark.agent.facets.OutputStatisticsFacet;
import openlineage.spark.agent.client.LineageEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.execution.metric.SQLMetric;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.PartialFunction;
import scala.PartialFunction$;
import scala.collection.Map;
import scala.runtime.AbstractFunction0;

/**
 * Utility functions for traversing a {@link
 * org.apache.spark.sql.catalyst.plans.logical.LogicalPlan}.
 */
@Slf4j
public class PlanUtils {

  /**
   * Merge a list of {@link PartialFunction}s and return the first value where the function is
   * defined or null if no function matches the input.
   *
   * @param fns
   * @param arg
   * @param <T>
   * @param <R>
   * @return
   */
  public static <T, R> R applyFirst(List<PartialFunction<T, R>> fns, T arg) {
    PartialFunction<T, R> fn = merge(fns);
    if (fn.isDefinedAt(arg)) {
      return fn.apply(arg);
    }
    return null;
  }

  /**
   * Given a list of {@link PartialFunction}s merge to produce a single function that will test the
   * input against each function one by one until a match is found or {@link
   * PartialFunction$#empty()} is returned.
   *
   * @param fns
   * @param <T>
   * @param <R>
   * @return
   */
  public static <T, R> PartialFunction<T, R> merge(List<PartialFunction<T, R>> fns) {
    return fns.stream().reduce((a, b) -> a.orElse(b)).orElse(PartialFunction$.MODULE$.empty());
  }

  /**
   * Given a schema, construct a valid {@link LineageEvent.SchemaDatasetFacet}.
   *
   * @param structType
   * @return
   */
  public static LineageEvent.SchemaDatasetFacet schemaFacet(StructType structType) {
    return LineageEvent.SchemaDatasetFacet.builder()
        ._producer(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        ._schemaURL(URI.create(OpenLineageClient.OPEN_LINEAGE_SCHEMA_FACET_URI))
        .fields(transformFields(structType.fields()))
        .build();
  }

  private static List<LineageEvent.SchemaField> transformFields(StructField[] fields) {
    List<LineageEvent.SchemaField> list = new ArrayList<>();
    for (StructField field : fields) {
      list.add(LineageEvent.SchemaField.builder().name(field.name()).type(field.dataType().typeName()).build());
    }
    return list;
  }

  public static String namespaceUri(URI outputPath) {
    return Optional.ofNullable(outputPath.getAuthority())
        .map(a -> String.format("%s://%s", outputPath.getScheme(), a))
        .orElse(outputPath.getScheme());
  }

  /**
   * Given a {@link URI}, construct a valid {@link LineageEvent.Dataset} following the expected naming
   * conventions.
   *
   * @param outputPath
   * @param schema
   * @return
   */
  public static LineageEvent.Dataset getDataset(URI outputPath, StructType schema) {
    String namespace = namespaceUri(outputPath);
    LineageEvent.DatasetFacet datasetFacet = datasetFacet(schema, namespace);
    return getDataset(outputPath, namespace, datasetFacet);
  }

  /**
   * Construct a dataset given a {@link URI}, namespace, and preconstructed {@link LineageEvent.DatasetFacet}.
   *
   * @param outputPath
   * @param namespace
   * @param datasetFacet
   * @return
   */
  public static LineageEvent.Dataset getDataset(URI outputPath, String namespace, LineageEvent.DatasetFacet datasetFacet) {
    return LineageEvent.Dataset.builder()
        .namespace(namespace)
        .name(outputPath.getPath())
        .facets(datasetFacet)
        .build();
  }

  /**
   * Construct a {@link LineageEvent.DatasetFacet} given a schema and a namespace.
   *
   * @param schema
   * @param namespaceUri
   * @return
   */
  public static LineageEvent.DatasetFacet datasetFacet(StructType schema, String namespaceUri) {
    return LineageEvent.DatasetFacet.builder()
        .schema(schemaFacet(schema))
        .dataSource(datasourceFacet(namespaceUri))
        .build();
  }

  /**
   * Construct a {@link LineageEvent.DatasetFacet} given a schema, a namespace, and an {@link
   * OutputStatisticsFacet}.
   *
   * @param schema
   * @param namespaceUri
   * @param outputStats
   * @return
   */
  public static LineageEvent.DatasetFacet datasetFacet(
      StructType schema, String namespaceUri, OutputStatisticsFacet outputStats) {
    return LineageEvent.DatasetFacet.builder()
        .schema(schemaFacet(schema))
        .dataSource(datasourceFacet(namespaceUri))
        .additional(ImmutableMap.of("stats", outputStats))
        .build();
  }

  /**
   * Construct a {@link LineageEvent.DatasourceDatasetFacet} given a namespace for the datasource.
   *
   * @param namespaceUri
   * @return
   */
  public static LineageEvent.DatasourceDatasetFacet datasourceFacet(String namespaceUri) {
    return LineageEvent.DatasourceDatasetFacet.builder()
        ._producer(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        ._schemaURL(URI.create(OpenLineageClient.OPEN_LINEAGE_DATASOURCE_FACET))
        .uri(namespaceUri)
        .name(namespaceUri)
        .build();
  }

  /**
   * Construct a {@link LineageEvent.ParentRunFacet} given the parent job's runId, job name, and namespace.
   *
   * @param runId
   * @param parentJob
   * @param parentJobNamespace
   * @return
   */
  public static LineageEvent.ParentRunFacet parentRunFacet(
      String runId, String parentJob, String parentJobNamespace) {
    return LineageEvent.ParentRunFacet.builder()
        ._producer(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        ._schemaURL(URI.create(OpenLineageClient.OPEN_LINEAGE_PARENT_FACET_URI))
        .run(LineageEvent.RunLink.builder().runId(runId).build())
        .job(LineageEvent.JobLink.builder().name(parentJob).namespace(parentJobNamespace).build())
        .build();
  }

  public static OutputStatisticsFacet getOutputStats(Map<String, SQLMetric> metrics) {
    long rowCount =
        metrics
            .getOrElse(
                "numOutputRows",
                new AbstractFunction0<SQLMetric>() {
                  @Override
                  public SQLMetric apply() {
                    return new SQLMetric("sum", 0L);
                  }
                })
            .value();
    long outputBytes =
        metrics
            .getOrElse(
                "numOutputBytes",
                new AbstractFunction0<SQLMetric>() {
                  @Override
                  public SQLMetric apply() {
                    return new SQLMetric("sum", 0L);
                  }
                })
            .value();
    return new OutputStatisticsFacet(rowCount, outputBytes);
  }

  static Path getDirectoryPath(Path p, Configuration hadoopConf) {
    try {
      if (p.getFileSystem(hadoopConf).getFileStatus(p).isFile()) {
        return p.getParent();
      } else {
        return p;
      }
    } catch (IOException e) {
      log.warn("Unable to get file system for path ", e);
      return p;
    }
  }
}
