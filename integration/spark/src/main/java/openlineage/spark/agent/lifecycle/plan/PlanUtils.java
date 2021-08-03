package openlineage.spark.agent.lifecycle.plan;

import io.openlineage.client.OpenLineage;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import openlineage.spark.agent.client.OpenLineageClient;
import openlineage.spark.agent.facets.OutputStatisticsFacet;
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
   * Given a schema, construct a valid {@link OpenLineage.SchemaDatasetFacet}.
   *
   * @param structType
   * @return
   */
  public static OpenLineage.SchemaDatasetFacet schemaFacet(StructType structType) {
    return new OpenLineage(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        .newSchemaDatasetFacetBuilder()
        .fields(transformFields(structType.fields()))
        .build();
  }

  private static List<OpenLineage.SchemaDatasetFacetFields> transformFields(StructField[] fields) {
    List<OpenLineage.SchemaDatasetFacetFields> list = new ArrayList<>();
    for (StructField field : fields) {
      list.add(
          new OpenLineage.SchemaDatasetFacetFieldsBuilder()
              .name(field.name())
              .type(field.dataType().typeName())
              .build());
    }
    return list;
  }

  public static String namespaceUri(URI outputPath) {
    return Optional.ofNullable(outputPath.getAuthority())
        .map(a -> String.format("%s://%s", outputPath.getScheme(), a))
        .orElse(outputPath.getScheme());
  }

  /**
   * Given a {@link URI}, construct a valid {@link OpenLineage.Dataset} following the expected
   * naming conventions.
   *
   * @param outputPath
   * @param schema
   * @return
   */
  public static OpenLineage.Dataset getDataset(URI outputPath, StructType schema) {
    String namespace = namespaceUri(outputPath);
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(schema, namespace);
    return getDataset(outputPath, namespace, datasetFacet);
  }

  /**
   * Construct a dataset given a {@link URI}, namespace, and preconstructed {@link
   * OpenLineage.DatasetFacets}.
   *
   * @param outputPath
   * @param namespace
   * @param datasetFacet
   * @return
   */
  public static OpenLineage.Dataset getDataset(
      URI outputPath, String namespace, OpenLineage.DatasetFacets datasetFacet) {
    return new OpenLineage.InputDatasetBuilder()
        .namespace(namespace)
        .name(outputPath.getPath())
        .facets(datasetFacet)
        .build();
  }

  /**
   * Construct a {@link OpenLineage.DatasetFacets} given a schema and a namespace.
   *
   * @param schema
   * @param namespaceUri
   * @return
   */
  public static OpenLineage.DatasetFacets datasetFacet(StructType schema, String namespaceUri) {
    return new OpenLineage.DatasetFacetsBuilder()
        .schema(schemaFacet(schema))
        .dataSource(datasourceFacet(namespaceUri))
        .build();
  }

  /**
   * Construct a {@link OpenLineage.DatasetFacets} given a schema, a namespace, and an {@link
   * OutputStatisticsFacet}.
   *
   * @param schema
   * @param namespaceUri
   * @param outputStats
   * @return
   */
  public static OpenLineage.DatasetFacets datasetFacet(
      StructType schema, String namespaceUri, OutputStatisticsFacet outputStats) {
    OpenLineage.DatasetFacetsBuilder builder =
        new OpenLineage.DatasetFacetsBuilder()
            .schema(schemaFacet(schema))
            .dataSource(datasourceFacet(namespaceUri));

    builder.put("stats", outputStats);
    return builder.build();
  }

  /**
   * Construct a {@link OpenLineage.DatasourceDatasetFacet} given a namespace for the datasource.
   *
   * @param namespaceUri
   * @return
   */
  public static OpenLineage.DatasourceDatasetFacet datasourceFacet(String namespaceUri) {
    return new OpenLineage(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        .newDatasourceDatasetFacetBuilder()
        .uri(URI.create(namespaceUri))
        .name(namespaceUri)
        .build();
  }

  /**
   * Construct a {@link OpenLineage.ParentRunFacet} given the parent job's runId, job name, and
   * namespace.
   *
   * @param runId
   * @param parentJob
   * @param parentJobNamespace
   * @return
   */
  public static OpenLineage.ParentRunFacet parentRunFacet(
      String runId, String parentJob, String parentJobNamespace) {
    return new OpenLineage(URI.create(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI))
        .newParentRunFacetBuilder()
        .run(
            new OpenLineage.ParentRunFacetRunBuilder()
                .runId(convertToUUID.apply(runId).orElse(UUID.randomUUID()))
                .build())
        .job(
            new OpenLineage.ParentRunFacetJobBuilder()
                .name(parentJob)
                .namespace(parentJobNamespace)
                .build())
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

  public static final Function<String, Optional<UUID>> convertToUUID = tryConvert(UUID::fromString);

  private static <T, R> Function<T, Optional<R>> tryConvert(Function<T, R> func) {
    return (t) -> {
      try {
        return Optional.ofNullable(func.apply(t));
      } catch (Exception e) {
        return Optional.empty();
      }
    };
  }
}
