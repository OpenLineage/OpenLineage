package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.client.OpenLineageClient;

import java.util.HashMap;


public class EnvironmentFacet extends OpenLineage.DefaultRunFacet {
    @JsonProperty("environment-properties")
    private HashMap<String, Object> properties;

    public EnvironmentFacet(HashMap<String, Object> environmentDetails) {
        super(OpenLineageClient.OPEN_LINEAGE_CLIENT_URI);
        this.properties = environmentDetails;
    }
}
