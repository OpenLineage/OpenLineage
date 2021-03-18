package marquez.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import marquez.spark.agent.client.LineageEvent.Dataset;
import marquez.spark.agent.client.LineageEvent.DatasetFacet;
import marquez.spark.agent.client.LineageEvent.DatasourceDatasetFacet;
import marquez.spark.agent.client.LineageEvent.JobLink;
import marquez.spark.agent.client.LineageEvent.ParentRunFacet;
import marquez.spark.agent.client.LineageEvent.RunLink;
import marquez.spark.agent.client.LineageEvent.SchemaDatasetFacet;
import marquez.spark.agent.client.LineageEvent.SchemaField;
import marquez.spark.agent.client.OpenLineageClient;
import marquez.spark.agent.facets.OutputStatisticsFacet;
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
   * Given a schema, construct a valid {@link SchemaDatasetFacet}.
   *
   * @param structType
   * @return
   */
  public static SchemaDatasetFacet schemaFacet(StructType structType) {
    return SchemaDatasetFacet.builder()
        ._producer(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        ._schemaURL(URI.create(OpenLineageClient.OPEN_LINEAGE_SCHEMA_FACET_URI))
        .fields(transformFields(structType.fields()))
        .build();
  }

  private static List<SchemaField> transformFields(StructField[] fields) {
    List<SchemaField> list = new ArrayList<>();
    for (StructField field : fields) {
      list.add(SchemaField.builder().name(field.name()).type(field.dataType().typeName()).build());
    }
    return list;
  }

  public static String namespaceUri(URI outputPath) {
    return Optional.ofNullable(outputPath.getAuthority())
        .map(a -> String.format("%s://%s", outputPath.getScheme(), a))
        .orElse(outputPath.getScheme());
  }

  /**
   * Given a {@link URI}, construct a valid {@link Dataset} following the expected naming
   * conventions.
   *
   * @param outputPath
   * @param schema
   * @return
   */
  public static Dataset getDataset(URI outputPath, StructType schema) {
    String namespace = namespaceUri(outputPath);
    DatasetFacet datasetFacet = datasetFacet(schema, namespace);
    return getDataset(outputPath, namespace, datasetFacet);
  }

  /**
   * Construct a dataset given a {@link URI}, namespace, and preconstructed {@link DatasetFacet}.
   *
   * @param outputPath
   * @param namespace
   * @param datasetFacet
   * @return
   */
  public static Dataset getDataset(URI outputPath, String namespace, DatasetFacet datasetFacet) {
    return Dataset.builder()
        .namespace(namespace)
        .name(outputPath.getPath())
        .facets(datasetFacet)
        .build();
  }

  /**
   * Construct a {@link DatasetFacet} given a schema and a namespace.
   *
   * @param schema
   * @param namespaceUri
   * @return
   */
  public static DatasetFacet datasetFacet(StructType schema, String namespaceUri) {
    return DatasetFacet.builder()
        .schema(schemaFacet(schema))
        .dataSource(datasourceFacet(namespaceUri))
        .build();
  }

  /**
   * Construct a {@link DatasetFacet} given a schema, a namespace, and an {@link
   * OutputStatisticsFacet}.
   *
   * @param schema
   * @param namespaceUri
   * @param outputStats
   * @return
   */
  public static DatasetFacet datasetFacet(
      StructType schema, String namespaceUri, OutputStatisticsFacet outputStats) {
    return DatasetFacet.builder()
        .schema(schemaFacet(schema))
        .dataSource(datasourceFacet(namespaceUri))
        .additional(ImmutableMap.of("stats", outputStats))
        .build();
  }

  /**
   * Construct a {@link DatasourceDatasetFacet} given a namespace for the datasource.
   *
   * @param namespaceUri
   * @return
   */
  public static DatasourceDatasetFacet datasourceFacet(String namespaceUri) {
    return DatasourceDatasetFacet.builder()
        ._producer(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        ._schemaURL(URI.create(OpenLineageClient.OPEN_LINEAGE_DATASOURCE_FACET))
        .uri(namespaceUri)
        .name(namespaceUri)
        .build();
  }

  /**
   * Construct a {@link ParentRunFacet} given the parent job's runId, job name, and namespace.
   *
   * @param runId
   * @param parentJob
   * @param parentJobNamespace
   * @return
   */
  public static ParentRunFacet parentRunFacet(
      String runId, String parentJob, String parentJobNamespace) {
    return ParentRunFacet.builder()
        ._producer(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        ._schemaURL(URI.create(OpenLineageClient.OPEN_LINEAGE_PARENT_FACET_URI))
        .run(RunLink.builder().runId(runId).build())
        .job(JobLink.builder().name(parentJob).namespace(parentJobNamespace).build())
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
}
