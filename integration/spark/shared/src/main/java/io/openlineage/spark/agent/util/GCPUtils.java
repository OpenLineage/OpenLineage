/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.CharStreams;
import io.openlineage.client.Environment;
import io.openlineage.spark.api.OpenLineageContext;
import io.openlineage.spark.api.naming.NameNormalizer;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.http.Consts;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
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
  private static final String JOB_ID_PREFIX = "dataproc_job_";
  private static final String JOB_UUID_PREFIX = "dataproc_uuid_";
  private static final String METADATA_FLAVOUR = "Metadata-Flavor";
  private static final String GOOGLE = "Google";
  private static final String SPARK_DIST_CLASSPATH = "SPARK_DIST_CLASSPATH";

  enum ResourceType {
    CLUSTER,
    BATCH,
    INTERACTIVE,
    UNKNOWN
  }

  static {
    RequestConfig config =
        RequestConfig.custom()
            .setConnectTimeout(100)
            .setConnectionRequestTimeout(100)
            .setSocketTimeout(100)
            .build();
    HTTP_CLIENT = HttpClients.custom().setDefaultRequestConfig(config).build();
  }

  public static boolean isDataprocRuntime() {
    String sparkDistClasspath = Environment.getEnvironmentVariable(SPARK_DIST_CLASSPATH);
    return (sparkDistClasspath != null && sparkDistClasspath.contains(DATAPROC_CLASSPATH));
  }

  public static Map<String, Object> getDataprocRunFacetMap(SparkContext context) {
    Map<String, Object> dataprocProperties = new HashMap<>();
    ResourceType resource = identifyResource(context);

    switch (resource) {
      case CLUSTER:
        getClusterName(context).ifPresent(p -> dataprocProperties.put("clusterName", p));
        getClusterUUID(context).ifPresent(p -> dataprocProperties.put("clusterUuid", p));
        getDataprocJobID(context).ifPresent(p -> dataprocProperties.put("jobId", p));
        getDataprocJobUUID(context).ifPresent(p -> dataprocProperties.put("jobUuid", p));
        break;
      case BATCH:
        getDataprocBatchID(context).ifPresent(p -> dataprocProperties.put("batchId", p));
        getDataprocBatchUUID(context).ifPresent(p -> dataprocProperties.put("batchUuid", p));
        break;
      case INTERACTIVE:
        getDataprocSessionID(context).ifPresent(p -> dataprocProperties.put("sessionId", p));
        getDataprocSessionUUID(context).ifPresent(p -> dataprocProperties.put("sessionUuid", p));
        break;
      case UNKNOWN:
        // do nothing
        break;
    }
    getGCPProjectId(context).ifPresent(p -> dataprocProperties.put("projectId", p));
    getSparkAppId(context).ifPresent(p -> dataprocProperties.put("appId", p));
    getSparkAppName(context).ifPresent(p -> dataprocProperties.put("appName", p));
    return dataprocProperties;
  }

  public static Map<String, Object> getOriginFacetMap(SparkContext sparkContext) {
    return createDataprocOriginMap(sparkContext);
  }

  public static Optional<String> getSparkQueryExecutionNodeName(OpenLineageContext context) {
    if (!context.getQueryExecution().isPresent()) return Optional.empty();

    SparkPlan node = context.getQueryExecution().get().executedPlan();
    if (node instanceof WholeStageCodegenExec) node = ((WholeStageCodegenExec) node).child();
    return Optional.of(NameNormalizer.normalize(node.nodeName()));
  }

  private static ResourceType identifyResource(SparkContext context) {
    if ("yarn".equals(context.getConf().get(SPARK_MASTER, ""))) return ResourceType.CLUSTER;
    if (getDataprocBatchID(context).isPresent()) return ResourceType.BATCH;
    if (getDataprocSessionID(context).isPresent()) return ResourceType.INTERACTIVE;

    return ResourceType.UNKNOWN;
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
    return getPropertyFromYarnTag(context, JOB_ID_PREFIX);
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

  private static Map<String, Object> createDataprocOriginMap(SparkContext context) {
    Map<String, Object> originProperties = new HashMap<>();
    String nameFormat = "";
    String resourceID = "";
    String regionName = getDataprocRegion(context).orElse("");
    String projectID = getGCPProjectId(context).orElse("");

    switch (identifyResource(context)) {
      case CLUSTER:
        nameFormat = "projects/%s/regions/%s/clusters/%s";
        resourceID = getClusterName(context).orElse("");
        break;
      case BATCH:
        nameFormat = "projects/%s/locations/%s/batches/%s";
        resourceID = getDataprocBatchID(context).orElse("");
        break;
      case INTERACTIVE:
        nameFormat = "projects/%s/locations/%s/sessions/%s";
        resourceID = getDataprocSessionID(context).orElse("");
        break;
      case UNKNOWN:
        nameFormat = "projects/%s/regions/%s/unknown/%s";
        break;
    }
    String dataprocResource = String.format(nameFormat, projectID, regionName, resourceID);
    originProperties.put("name", dataprocResource);
    originProperties.put("sourceType", "DATAPROC");
    return originProperties;
  }

  private static Optional<String> getPropertyFromYarnTag(SparkContext context, String tagPrefix) {
    String yarnTag = context.getConf().get(SPARK_YARN_TAGS, null);
    if (yarnTag == null) {
      return Optional.empty();
    }
    return Arrays.stream(yarnTag.split(","))
        .filter(tag -> tag.contains(tagPrefix))
        .findFirst()
        .map(tag -> tag.substring(tagPrefix.length()));
  }

  private static Optional<String> fetchGCPMetadata(String httpEndpoint, SparkContext context) {
    String baseUri = context.getConf().get(GOOGLE_METADATA_API, BASE_URI);
    String httpURI = baseUri + httpEndpoint;
    HttpGet httpGet = new HttpGet(httpURI);
    httpGet.addHeader(METADATA_FLAVOUR, GOOGLE);
    try (CloseableHttpResponse response = HTTP_CLIENT.execute(httpGet)) {
      handleError(response);
      return Optional.of(
          CharStreams.toString(new InputStreamReader(response.getEntity().getContent(), UTF_8)));
    } catch (IOException e) {
      return Optional.empty();
    }
  }

  private static void handleError(HttpResponse response) throws IOException {
    final int statusCode = response.getStatusLine().getStatusCode();
    if (statusCode < 400 || statusCode >= 600) return;
    String message =
        String.format(
            "code: %d, response: %s",
            statusCode, EntityUtils.toString(response.getEntity(), Consts.UTF_8));
    throw new IOException(message);
  }
}
