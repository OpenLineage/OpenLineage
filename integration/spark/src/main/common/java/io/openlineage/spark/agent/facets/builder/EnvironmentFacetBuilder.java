package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.api.CustomFacetBuilder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.*;
import java.util.function.BiConsumer;

import io.openlineage.spark.api.OpenLineageContext;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.json.simple.parser.JSONParser;

public class EnvironmentFacetBuilder extends CustomFacetBuilder<SparkListenerEvent, EnvironmentFacet> {
    private static HashMap<String, Object> dbProperties;
    private static final org.slf4j.Logger log
            = org.slf4j.LoggerFactory.getLogger(EnvironmentFacetBuilder.class);
    private final OpenLineageContext openLineageContext;

    public EnvironmentFacetBuilder(OpenLineageContext openLineageContext) {
        this.openLineageContext = openLineageContext;
    }

    @Override
    protected void build(SparkListenerEvent event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
        if(System.getenv().containsKey("DATABRICKS_RUNTIME_VERSION")) {
            consumer.accept("environment-properties", new EnvironmentFacet(getDatabricksEnvironmentalAttributes(event)));
        }
    }

    private HashMap<String, Object> getDatabricksEnvironmentalAttributes(SparkListenerEvent jobStart) {
        dbProperties = new HashMap<>();
        String urlString = "http://" + openLineageContext.getProperties().getProperty("spark.databricks.clusterUsageTags.driverInstancePrivateIp") + ":7070/?type=%22com.databricks.backend.daemon.data.common.DataMessages$GetMountsV2%22";

        List<String> dbPropertiesKeys = Arrays.asList(
                "orgId",
                "spark.databricks.clusterUsageTags.clusterOwnerOrgId",
                "spark.databricks.notebook.path",
                "spark.databricks.job.type",
                "spark.databricks.job.id",
                "spark.databricks.job.runId",
                "user",
                "userId",
                "spark.databricks.clusterUsageTags.clusterName",
                "spark.databricks.clusterUsageTags.azureSubscriptionId"
        );

        dbPropertiesKeys.stream().forEach((p) -> {
            dbProperties.put(p, openLineageContext.getProperties().getProperty(p));
        });

        dbProperties.put("mountPoints", getDatabricksMountpoints(urlString));

        return dbProperties;
    }

    private static List<Map<String, String>> getDatabricksMountpoints(String urlString) {
        List<Map<String, String>> mapList = new ArrayList<>();
        try {
            URL url = new URL(urlString);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Accept", "application/json");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Sessionid", "1234");
            conn.setRequestProperty("Auth", "{}");
            conn.setRequestProperty("authType", "com.databricks.backend.daemon.data.common.DbfsAuth");
            String jsonInputString = "{}";
            conn.setDoOutput(true);
            conn.setDoInput(true);

            conn.getOutputStream().write(jsonInputString.getBytes("UTF-8"));
            Reader in = new BufferedReader(new InputStreamReader(conn.getInputStream(), "UTF-8"));
            StringBuilder sb = new StringBuilder();
            for (int c; (c = in.read()) >= 0; )
                sb.append((char) c);

            JSONParser parser = new JSONParser();
            org.json.simple.JSONArray jsonArray = (org.json.simple.JSONArray) parser.parse(sb.toString());

            jsonArray.forEach(x -> {
                HashMap<String, String> values = new HashMap<>();

                values.put("MountPoint", ((org.json.simple.JSONObject) x).get("mountPointString").toString());
                values.put("Source", ((org.json.simple.JSONObject) x).get("sourceString").toString());

                mapList.add(values);
            });

            conn.disconnect();

        } catch (Exception e) {
            log.warn("Error while getting mount points");
        }
        return mapList;
    }
}
