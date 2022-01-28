package io.openlineage.spark.agent.facets.builder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.agent.models.DatabricksMountpoint;
import io.openlineage.spark.api.CustomFacetBuilder;
import io.openlineage.spark.api.OpenLineageContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.function.BiConsumer;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.spark.scheduler.SparkListenerEvent;

/**
 * {@link CustomFacetBuilder} that generates a {@link EnvironmentFacet} when using OpenLineage on
 * Databricks.
 */
public class DatabricksEnvironmentFacetBuilder
    extends CustomFacetBuilder<SparkListenerEvent, EnvironmentFacet> {
  private static HashMap<String, Object> dbProperties;
  private static final org.slf4j.Logger log =
      org.slf4j.LoggerFactory.getLogger(DatabricksEnvironmentFacetBuilder.class);
  private final OpenLineageContext openLineageContext;

  public DatabricksEnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
    this.openLineageContext = openLineageContext;
  }

  public static boolean isDatabricksRuntime() {
    return System.getenv().containsKey("DATABRICKS_RUNTIME_VERSION");
  }

  @Override
  protected void build(
      SparkListenerEvent event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    consumer.accept(
        "environment-properties",
        new EnvironmentFacet(getDatabricksEnvironmentalAttributes(event)));
  }

  private HashMap<String, Object> getDatabricksEnvironmentalAttributes(
      SparkListenerEvent jobStart) {
    dbProperties = new HashMap<>();
    String urlString =
        "http://"
            + openLineageContext
                .getProperties()
                .getProperty("spark.databricks.clusterUsageTags.driverInstancePrivateIp")
            + ":7070/?type=%22com.databricks.backend.daemon.data.common.DataMessages$GetMountsV2%22";

    List<String> dbPropertiesKeys =
        Arrays.asList(
            "orgId",
            "spark.databricks.clusterUsageTags.clusterOwnerOrgId",
            "spark.databricks.notebook.path",
            "spark.databricks.job.type",
            "spark.databricks.job.id",
            "spark.databricks.job.runId",
            "user",
            "userId",
            "spark.databricks.clusterUsageTags.clusterName",
            "spark.databricks.clusterUsageTags.azureSubscriptionId");

    dbPropertiesKeys.stream()
        .forEach(
            (p) -> {
              dbProperties.put(p, openLineageContext.getProperties().getProperty(p));
            });

    dbProperties.put("mountPoints", getDatabricksMountpoints(urlString));

    return dbProperties;
  }

  private static List<DatabricksMountpoint> getDatabricksMountpoints(String urlString) {
    List<DatabricksMountpoint> mountpoints = new ArrayList<>();
    try {
      String result = "";
      HttpPost post = new HttpPost(urlString);
      post.addHeader("Sessionid", "1234");
      post.addHeader("Auth", "{}");
      post.addHeader("authType", "com.databricks.backend.daemon.data.common.DbfsAuth");

      post.setEntity(new StringEntity("{}"));

      CloseableHttpClient httpClient = HttpClients.createDefault();
      CloseableHttpResponse response = httpClient.execute(post);

      result = EntityUtils.toString(response.getEntity());

      mountpoints = jsonArrayToObjectList(result, DatabricksMountpoint.class);
    } catch (Exception e) {
      log.warn(e.getMessage());
    }
    return mountpoints;
  }

  public static <T> List<T> jsonArrayToObjectList(String json, Class<T> tClass) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    CollectionType listType =
        mapper.getTypeFactory().constructCollectionType(ArrayList.class, tClass);
    List<T> ts = mapper.readValue(json, listType);
    return ts;
  }
}
