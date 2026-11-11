/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.openlineage.client.OpenLineage;
import io.openlineage.spark.agent.Versions;
import io.openlineage.spark.api.OpenLineageContext;
import org.junit.jupiter.api.Test;

class DatasetFacetsUtilsTest {
  private final OpenLineage openLineage = new OpenLineage(Versions.OPEN_LINEAGE_PRODUCER_URI);

  @Test
  void testCopyToBuilderKeepsOneLakeFacet() {
    OpenLineageContext context = mock(OpenLineageContext.class);
    when(context.getOpenLineage()).thenReturn(openLineage);
    OpenLineage.FabricOneLakeDatasetFacet oneLake =
        openLineage
            .newFabricOneLakeDatasetFacetBuilder()
            .workspaceId("d1f6473b-9f7a-4352-940b-a8d74461d3fc")
            .itemName("wh_primary")
            .itemType("Warehouse")
            .build();
    OpenLineage.DatasetFacets facets =
        openLineage.newDatasetFacetsBuilder().fabric_onelake(oneLake).build();

    // what SaveIntoDataSourceCommandVisitor does when it marks a table as overwritten
    OpenLineage.DatasetFacets rebuilt =
        DatasetFacetsUtils.copyToBuilder(context, facets)
            .lifecycleStateChange(
                openLineage.newLifecycleStateChangeDatasetFacet(
                    OpenLineage.LifecycleStateChangeDatasetFacet.LifecycleStateChange.OVERWRITE,
                    null))
            .build();

    assertThat(rebuilt.getFabric_onelake()).isSameAs(oneLake);
    assertThat(rebuilt.getLifecycleStateChange()).isNotNull();
  }
}
