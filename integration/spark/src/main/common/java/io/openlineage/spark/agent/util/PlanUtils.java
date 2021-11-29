package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
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
  public static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
      "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
  public static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
      "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";

  /**
   * Merge a list of {@link PartialFunction}s and return the first value where the function is
   * defined or empty list if no function matches the input.
   *
   * @param fns
   * @param arg
   * @param <T>
   * @param <R>
   * @return
   */
  public static <T, R> List<R> applyFirst(List<? extends PartialFunction<T, List<R>>> fns, T arg) {
    PartialFunction<T, List<R>> fn = merge(fns);
    if (fn.isDefinedAt(arg)) {
      return fn.apply(arg);
    }
    return Collections.emptyList();
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
  public static <T, R> PartialFunction<T, R> merge(List<? extends PartialFunction<T, R>> fns) {
    return fns.stream()
        .map(PartialFunction.class::cast)
        .reduce(PartialFunction::orElse)
        .orElse(PartialFunction$.MODULE$.empty());
  }

  /**
   * Given a schema, construct a valid {@link OpenLineage.SchemaDatasetFacet}.
   *
   * @param structType
   * @return
   */
  public static OpenLineage.SchemaDatasetFacet schemaFacet(StructType structType) {
    return new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
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
   * Construct a dataset {@link OpenLineage.Dataset} given a name, namespace, and preconstructed
   * {@link OpenLineage.DatasetFacets}.
   *
   * @param name
   * @param namespace
   * @param datasetFacet
   * @return
   */
  public static OpenLineage.Dataset getDataset(
      String name, String namespace, OpenLineage.DatasetFacets datasetFacet) {
    return new OpenLineage.Dataset() {
      @Override
      public String getNamespace() {
        return namespace;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public OpenLineage.DatasetFacets getFacets() {
        return datasetFacet;
      }
    };
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
    return getDataset(outputPath.getPath(), namespace, datasetFacet);
  }

  public static OpenLineage.Dataset getDataset(DatasetIdentifier ident, StructType schema) {
    OpenLineage.DatasetFacets datasetFacet = datasetFacet(schema, ident.getNamespace());
    return getDataset(ident.getName(), ident.getNamespace(), datasetFacet);
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
   * Construct a {@link OpenLineage.DatasourceDatasetFacet} given a namespace for the datasource.
   *
   * @param namespaceUri
   * @return
   */
  public static OpenLineage.DatasourceDatasetFacet datasourceFacet(String namespaceUri) {
    return new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
        .newDatasourceDatasetFacetBuilder()
        .uri(URI.create(namespaceUri))
        .name(namespaceUri)
        .build();
  }

  /**
   * Construct a {@link OpenLineage.ParentRunFacet} given the parent job's parentRunId, job name,
   * and namespace.
   *
   * @param parentRunId
   * @param parentJob
   * @param parentJobNamespace
   * @return
   */
  public static OpenLineage.ParentRunFacet parentRunFacet(
      UUID parentRunId, String parentJob, String parentJobNamespace) {
    return new OpenLineage(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI)
        .newParentRunFacetBuilder()
        .run(new OpenLineage.ParentRunFacetRunBuilder().runId(parentRunId).build())
        .job(
            new OpenLineage.ParentRunFacetJobBuilder()
                .name(parentJob)
                .namespace(parentJobNamespace)
                .build())
        .build();
  }

  public static OpenLineage.OutputStatisticsOutputDatasetFacet getOutputStats(
      OpenLineage ol, Map<String, SQLMetric> metrics) {
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
    return ol.newOutputStatisticsOutputDatasetFacet(rowCount, outputBytes);
  }

  public static Path getDirectoryPath(Path p, Configuration hadoopConf) {
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

  /**
   * JdbcUrl can contain username and password this method clean-up credentials from jdbcUrl and
   * strip the jdbc prefix from the url
   */
  public static String sanitizeJdbcUrl(String jdbcUrl) {
    jdbcUrl = jdbcUrl.substring(5);
    return jdbcUrl
        .replaceAll(SLASH_DELIMITER_USER_PASSWORD_REGEX, "@")
        .replaceAll(COLON_DELIMITER_USER_PASSWORD_REGEX, "$1")
        .replaceAll("(?<=[?,;&:)=])\\(?(?i)(?:user|username|password)=[^;&,)]+(?:[;&;)]|$)", "");
  }
}
