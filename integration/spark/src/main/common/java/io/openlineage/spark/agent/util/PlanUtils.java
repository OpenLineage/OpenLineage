/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd;
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.PartialFunction;
import scala.PartialFunction$;
import scala.runtime.AbstractPartialFunction;

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
  public static <T, R> PartialFunction<T, R> merge(
      Collection<? extends PartialFunction<T, R>> fns) {
    return fns.stream()
        .map(
            pfn ->
                new AbstractPartialFunction<T, R>() {
                  @Override
                  public boolean isDefinedAt(T x) {
                    try {
                      return pfn.isDefinedAt(x);
                    } catch (ClassCastException e) {
                      return false;
                    }
                  }

                  @Override
                  public R apply(T x) {
                    return pfn.apply(x);
                  }
                })
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
  public static OpenLineage.SchemaDatasetFacet schemaFacet(
      OpenLineage openLineage, StructType structType) {
    return openLineage
        .newSchemaDatasetFacetBuilder()
        .fields(transformFields(openLineage, structType.fields()))
        .build();
  }

  private static List<OpenLineage.SchemaDatasetFacetFields> transformFields(
      OpenLineage openLineage, StructField[] fields) {
    List<OpenLineage.SchemaDatasetFacetFields> list = new ArrayList<>();
    for (StructField field : fields) {
      list.add(
          openLineage
              .newSchemaDatasetFacetFieldsBuilder()
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
   * Construct a {@link OpenLineage.DatasourceDatasetFacet} given a namespace for the datasource.
   *
   * @param namespaceUri
   * @return
   */
  public static OpenLineage.DatasourceDatasetFacet datasourceFacet(
      OpenLineage openLineage, String namespaceUri) {
    return openLineage
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

  public static boolean shouldIncludeDatasetVersionFacet(
      boolean isInputVisitor, SparkListenerEvent event) {
    boolean isStartEvent =
        (event instanceof SparkListenerSQLExecutionStart || event instanceof SparkListenerJobStart);
    boolean isEndEvent =
        (event instanceof SparkListenerSQLExecutionEnd || event instanceof SparkListenerJobEnd);
    return (isInputVisitor && isStartEvent || !isInputVisitor && isEndEvent);
  }
}
