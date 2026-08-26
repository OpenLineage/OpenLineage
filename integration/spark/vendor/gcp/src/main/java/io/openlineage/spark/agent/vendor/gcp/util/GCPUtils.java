/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.vendor.gcp.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.openlineage.client.Environment;
import io.openlineage.spark.agent.util.ApplicationMetadataCache;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.naming.NameNormalizer;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.util.Timeout;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.WholeStageCodegenExec;

/** Util to extract values from GCP environment */
public class GCPUtils {

  private static final String BASE_URI = "http://metadata.google.internal/computeMetadata/v1";
  public static final String PROJECT_ID_ENDPOINT = "/project/project-id";
  public static final String BATCH_ID_ENDPOINT = "/instance/attributes/dataproc-batch-id";
  public static final String BATCH_UUID_ENDPOINT = "/instance/attributes/dataproc-batch-uuid";
  public static final String SESSION_ID_ENDPOINT = "/instance/attributes/dataproc-session-id";
  public static final String SESSION_UUID_ENDPOINT = "/instance/attributes/dataproc-session-uuid";
  public static final String CLUSTER_UUID_ENDPOINT = "/instance/attributes/dataproc-cluster-uuid";
  public static final String DATAPROC_REGION_ENDPOINT = "/instance/attributes/dataproc-region";
  private static final String DATAPROC_CLASSPATH = "/usr/local/share/google/dataproc/lib";
  private static final CloseableHttpClient HTTP_CLIENT;
  public static final String SPARK_YARN_TAGS = "spark.yarn.tags";
  public static final String SPARK_DRIVER_HOST = "spark.driver.host";
  public static final String SPARK_APP_ID = "spark.app.id";
  public static final String SPARK_APP_NAME = "spark.app.name";
  public static final String GOOGLE_METADATA_API = "google.metadata.api.base-url";
  public static final String SPARK_MASTER = "spark.master";
  private static final String JOB_ATTEMPT_TIMESTAMP_PREFIX = "dataproc_job_attempt_timestamp_";
  private static final String JOB_ID_PREFIX = "dataproc_job_";
  private static final String JOB_UUID_PREFIX = "dataproc_uuid_";
  private static final String METADATA_FLAVOUR = "Metadata-Flavor";
  private static final String GOOGLE = "Google";
  private static final String SPARK_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH";
  private static final String DATAPROC_METADATA_CACHE_KEY = "gcp.dataproc.application-metadata";

  enum ResourceType {
    CLUSTER,
    BATCH,
    INTERACTIVE,
    UNKNOWN
  }

  static {
    ConnectionConfig connectionConfig =
        ConnectionConfig.custom()
            .setConnectTimeout(Timeout.ofMilliseconds(100))
            .setSocketTimeout(Timeout.ofMilliseconds(100))
            .build();
    PoolingHttpClientConnectionManager connMan =
        PoolingHttpClientConnectionManagerBuilder.create()
            .setDefaultConnectionConfig(connectionConfig)
            .build();
    RequestConfig config =
        RequestConfig.custom().setConnectionRequestTimeout(Timeout.ofMilliseconds(100)).build();
    HTTP_CLIENT =
        HttpClients.custom().setDefaultRequestConfig(config).setConnectionManager(connMan).build();
  }

  public static boolean isDataprocRuntime() {
    String sparkDistClasspath = Environment.getEnvironmentVariable(SPARK_DIST_CLASSPATH);
    return (sparkDistClasspath != null && sparkDistClasspath.contains(DATAPROC_CLASSPATH));
  }

  // Remove suppression after PMD is updated to >=7.0.0
  @SuppressWarnings("PMD.SwitchStmtsShouldHaveDefault")
  public static Map<String, Object> getDataprocRunFacetMap(SparkContext context) {
    return new HashMap<>(getDataprocApplicationMetadata(context).getRunFacetProperties());
  }

  public static Map<String, Object> getOriginFacetMap(SparkContext sparkContext) {
    return new HashMap<>(getDataprocApplicationMetadata(sparkContext).getOriginProperties());
  }

  private static DataprocApplicationMetadata getDataprocApplicationMetadata(SparkContext context) {
    return ApplicationMetadataCache.forSparkContext(context)
        .get(DATAPROC_METADATA_CACHE_KEY, () -> loadDataprocApplicationMetadata(context));
  }

  // Remove suppression after PMD is updated to >=7.0.0
  @SuppressWarnings("PMD.SwitchStmtsShouldHaveDefault")
  private static DataprocApplicationMetadata loadDataprocApplicationMetadata(SparkContext context) {
    Optional<String> batchId = Optional.empty();
    Optional<String> sessionId = Optional.empty();
    ResourceType resource;
    if ("yarn".equals(context.getConf().get(SPARK_MASTER, ""))) {
      resource = ResourceType.CLUSTER;
    } else {
      batchId = getDataprocBatchID(context);
      if (batchId.isPresent()) {
        resource = ResourceType.BATCH;
      } else {
        sessionId = getDataprocSessionID(context);
        resource = sessionId.isPresent() ? ResourceType.INTERACTIVE : ResourceType.UNKNOWN;
      }
    }

    Optional<String> projectId = getGCPProjectId(context);
    Optional<String> region = getDataprocRegion(context);
    Map<String, Object> dataprocProperties = new HashMap<>();

    switch (resource) {
      case CLUSTER:
        getClusterName(context).ifPresent(p -> dataprocProperties.put("clusterName", p));
        getClusterUUID(context).ifPresent(p -> dataprocProperties.put("clusterUuid", p));
        getDataprocJobID(context).ifPresent(p -> dataprocProperties.put("jobId", p));
        getDataprocJobUUID(context).ifPresent(p -> dataprocProperties.put("jobUuid", p));
        dataprocProperties.put("jobType", "dataproc_job");
        break;
      case BATCH:
        batchId.ifPresent(p -> dataprocProperties.put("batchId", p));
        getDataprocBatchUUID(context).ifPresent(p -> dataprocProperties.put("batchUuid", p));
        dataprocProperties.put("jobType", "batch");
        break;
      case INTERACTIVE:
        sessionId.ifPresent(p -> dataprocProperties.put("sessionId", p));
        getDataprocSessionUUID(context).ifPresent(p -> dataprocProperties.put("sessionUuid", p));
        dataprocProperties.put("jobType", "session");
        break;
      case UNKNOWN:
        // do nothing
        break;
    }
    projectId.ifPresent(p -> dataprocProperties.put("projectId", p));
    getSparkAppId(context).ifPresent(p -> dataprocProperties.put("appId", p));
    getSparkAppName(context).ifPresent(p -> dataprocProperties.put("appName", p));

    Map<String, Object> originProperties = new HashMap<>();
    String nameFormat = "";
    String resourceId = "";
    switch (resource) {
      case CLUSTER:
        nameFormat = "projects/%s/regions/%s/clusters/%s";
        resourceId = getClusterName(context).orElse("");
        break;
      case BATCH:
        nameFormat = "projects/%s/locations/%s/batches/%s";
        resourceId = batchId.orElse("");
        break;
      case INTERACTIVE:
        nameFormat = "projects/%s/locations/%s/sessions/%s";
        resourceId = sessionId.orElse("");
        break;
      case UNKNOWN:
        nameFormat = "projects/%s/regions/%s/unknown/%s";
        break;
    }
    originProperties.put(
        "name", String.format(nameFormat, projectId.orElse(""), region.orElse(""), resourceId));
    originProperties.put("sourceType", "DATAPROC");

    return new DataprocApplicationMetadata(dataprocProperties, originProperties);
  }

  public static Optional<String> getSparkQueryExecutionNodeName(OpenLineageContext context) {
    if (!context.getQueryExecution().isPresent()) return Optional.empty();

    SparkPlan node = context.getQueryExecution().get().executedPlan();
    if (node instanceof WholeStageCodegenExec) node = ((WholeStageCodegenExec) node).child();
    return Optional.of(NameNormalizer.normalize(node.nodeName()));
  }

  private static Optional<String> getDriverHost(SparkContext context) {
    return Optional.ofNullable(context.getConf().get(SPARK_DRIVER_HOST));
  }

  /* sample hostname:
   * sample-cluster-m.us-central1-a.c.hadoop-cloud-dev.google.com.internal */
  private static Optional<String> getClusterName(SparkContext context) {
    return getDriverHost(context)
        .map(host -> host.split("\\.")[0])
        .map(s -> s.substring(0, s.lastIndexOf("-")));
  }

  private static Optional<String> getDataprocRegion(SparkContext context) {
    return fetchGCPMetadata(DATAPROC_REGION_ENDPOINT, context);
  }

  private static Optional<String> getDataprocJobID(SparkContext context) {
    return getPropertyFromYarnTag(context, JOB_ID_PREFIX, JOB_ATTEMPT_TIMESTAMP_PREFIX);
  }

  private static Optional<String> getDataprocJobUUID(SparkContext context) {
    return getPropertyFromYarnTag(context, JOB_UUID_PREFIX);
  }

  private static Optional<String> getDataprocBatchID(SparkContext context) {
    return fetchGCPMetadata(BATCH_ID_ENDPOINT, context);
  }

  private static Optional<String> getDataprocBatchUUID(SparkContext context) {
    return fetchGCPMetadata(BATCH_UUID_ENDPOINT, context);
  }

  private static Optional<String> getDataprocSessionID(SparkContext context) {
    return fetchGCPMetadata(SESSION_ID_ENDPOINT, context);
  }

  private static Optional<String> getDataprocSessionUUID(SparkContext context) {
    return fetchGCPMetadata(SESSION_UUID_ENDPOINT, context);
  }

  private static Optional<String> getGCPProjectId(SparkContext context) {
    return fetchGCPMetadata(PROJECT_ID_ENDPOINT, context)
        .map(b -> b.substring(b.lastIndexOf('/') + 1));
  }

  private static Optional<String> getSparkAppId(SparkContext context) {
    return Optional.ofNullable(context.getConf().get(SPARK_APP_ID));
  }

  private static Optional<String> getSparkAppName(SparkContext context) {
    return Optional.ofNullable(context.getConf().get(SPARK_APP_NAME));
  }

  private static Optional<String> getClusterUUID(SparkContext context) {
    return fetchGCPMetadata(CLUSTER_UUID_ENDPOINT, context);
  }

  private static Optional<String> getPropertyFromYarnTag(SparkContext context, String tagPrefix) {
    String yarnTag = context.getConf().get(SPARK_YARN_TAGS, null);
    if (yarnTag == null) {
      return Optional.empty();
    }
    return Arrays.stream(yarnTag.split(","))
        .filter(tag -> tag.startsWith(tagPrefix))
        .findFirst()
        .map(tag -> tag.substring(tagPrefix.length()));
  }

  private static Optional<String> getPropertyFromYarnTag(
      SparkContext context, String tagPrefix, String excludePrefix) {
    String yarnTag = context.getConf().get(SPARK_YARN_TAGS, null);
    if (yarnTag == null) {
      return Optional.empty();
    }
    return Arrays.stream(yarnTag.split(","))
        .filter(tag -> tag.startsWith(tagPrefix) && !tag.startsWith(excludePrefix))
        .findFirst()
        .map(tag -> tag.substring(tagPrefix.length()));
  }

  private static Optional<String> fetchGCPMetadata(String httpEndpoint, SparkContext context) {
    String baseUri = context.getConf().get(GOOGLE_METADATA_API, BASE_URI);
    String httpURI = baseUri + httpEndpoint;
    HttpGet httpGet = new HttpGet(httpURI);
    httpGet.addHeader(METADATA_FLAVOUR, GOOGLE);
    try {
      return HTTP_CLIENT.execute(
          httpGet,
          response -> {
            handleError(response);
            return Optional.of(EntityUtils.toString(response.getEntity()));
          });
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private static void handleError(ClassicHttpResponse response) throws IOException, ParseException {
    final int statusCode = response.getCode();
    if (statusCode < 400 || statusCode >= 600) return;
    String message =
        String.format(
            "code: %d, response: %s",
            statusCode, EntityUtils.toString(response.getEntity(), UTF_8));
    throw new IOException(message);
  }

  private static final class DataprocApplicationMetadata {
    private final Map<String, Object> runFacetProperties;
    private final Map<String, Object> originProperties;

    private DataprocApplicationMetadata(
        Map<String, Object> runFacetProperties, Map<String, Object> originProperties) {
      this.runFacetProperties = Collections.unmodifiableMap(new HashMap<>(runFacetProperties));
      this.originProperties = Collections.unmodifiableMap(new HashMap<>(originProperties));
    }

    private Map<String, Object> getRunFacetProperties() {
      return runFacetProperties;
    }

    private Map<String, Object> getOriginProperties() {
      return originProperties;
    }
  }
}
