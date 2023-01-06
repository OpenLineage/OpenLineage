/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets;

import com.fasterxml.jackson.annotation.JsonRawValue;
import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import java.util.List;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.Value;

@Value
@EqualsAndHashCode(callSuper = true)
public class UnknownEntryFacet extends OpenLineage.DefaultRunFacet {

  public UnknownEntryFacet(FacetEntry output, List<FacetEntry> inputs) {
    super(Versions.OPEN_LINEAGE_PRODUCER_URI);
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
