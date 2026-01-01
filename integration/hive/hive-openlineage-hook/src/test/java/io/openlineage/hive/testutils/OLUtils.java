/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.hive.testutils;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.hive.client.Versions;
import io.openlineage.hive.facets.HivePropertiesFacet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.common.util.HiveVersionInfo;

public class OLUtils {

  public static void assertDatasets(
      List<? extends OpenLineage.Dataset> datasets, Set<Map<String, Object>> expectedDatasets) {
    assertThat(datasets).hasSameSizeAs(expectedDatasets);
    Set<Map<String, Object>> actualDatasets = new HashSet<>();
    for (OpenLineage.Dataset dataset : datasets) {
      assertThat(dataset.getNamespace()).isEqualTo("file");
      String[] splitName = dataset.getName().split("/");
      String name = splitName[splitName.length - 1];
      assertSymlink(dataset.getFacets().getSymlinks(), asList("hive", "default." + name));
      List<String> actualSchema = new ArrayList<>();
      for (OpenLineage.SchemaDatasetFacetFields field :
          dataset.getFacets().getSchema().getFields()) {
        actualSchema.add(field.getName() + ":" + field.getType());
      }
      actualDatasets.add(createDatasetMap(name, actualSchema));
    }
    assertThat(actualDatasets).containsExactlyInAnyOrderElementsOf(expectedDatasets);
  }

  public static Map<String, Object> createDatasetMap(String name, List<String> schema) {
    Map<String, Object> map = new HashMap<>();
    map.put("name", name);
    map.put("schema", schema);
    return map;
  }

  public static void assertSymlink(OpenLineage.SymlinksDatasetFacet facet, List<String> expected) {
    assertThat(expected).hasSize(2);
    assertThat(facet.getIdentifiers()).hasSize(1);
    assertThat(facet.getIdentifiers().get(0).getNamespace()).isEqualTo(expected.get(0));
    assertThat(facet.getIdentifiers().get(0).getName()).isEqualTo(expected.get(1));
    assertThat(facet.getIdentifiers().get(0).getType()).isEqualTo("TABLE");
  }

  public static void assertStandardFormat(
      OpenLineage.RunEvent runEvent, HiveConf hiveConf, String olJobNamespace, String olJobName) {
    assertThat(runEvent.getEventType()).isEqualTo(OpenLineage.RunEvent.EventType.COMPLETE);
    assertThat(runEvent.getProducer().getPath()).endsWith("/integration/hive");
    assertThat(runEvent.getJob().getNamespace()).isEqualTo(olJobNamespace);
    assertThat(runEvent.getJob().getName()).isEqualTo(olJobName);
    OpenLineage.ProcessingEngineRunFacet processingEngine =
        (OpenLineage.ProcessingEngineRunFacet)
            runEvent.getRun().getFacets().getAdditionalProperties().get("processing_engine");
    assertThat(processingEngine.get_producer().getPath()).endsWith("/integration/hive");
    assertThat(processingEngine.getVersion()).isEqualTo(HiveVersionInfo.getVersion());
    assertThat(processingEngine.getName()).isEqualTo("hive");
    assertThat(processingEngine.getOpenlineageAdapterVersion()).isEqualTo(Versions.getVersion());
    HivePropertiesFacet hiveProperties =
        (HivePropertiesFacet)
            runEvent.getRun().getFacets().getAdditionalProperties().get("hive_properties");
    assertThat(hiveProperties.getProperties()).hasSize(2);
    assertThat(hiveProperties.getProperties().get("hive.execution.engine"))
        .isEqualTo(hiveConf.get("hive.execution.engine"));
    assertThat((String) hiveProperties.getProperties().get("hive.query.id")).isNotEmpty();
  }
}
