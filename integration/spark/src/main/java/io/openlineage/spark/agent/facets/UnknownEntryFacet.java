package io.openlineage.spark.agent.facets;

import static io.openlineage.spark.agent.client.OpenLineageClient.OPEN_LINEAGE_CLIENT_URI;

import com.fasterxml.jackson.annotation.JsonRawValue;
import io.openlineage.client.OpenLineage;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class UnknownEntryFacet extends OpenLineage.CustomFacet {

  public UnknownEntryFacet(FacetEntry output, List<FacetEntry> inputs) {
    super(OPEN_LINEAGE_CLIENT_URI);
    this.output = output;
    this.inputs = inputs;
  }

  FacetEntry output;
  List<FacetEntry> inputs;

  @Value
  public static class AttributeField {
    String name;
    String type;
    Map<String, Object> metadata;
  }

  @Value
  public static class FacetEntry {
    @JsonRawValue String description;
    List<AttributeField> inputAttributes;
    List<AttributeField> outputAttributes;
  }
}
