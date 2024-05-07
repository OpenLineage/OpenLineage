/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.DatasetVersionDatasetFacet;
import io.openlineage.client.OpenLineage.DatasourceDatasetFacet;
import io.openlineage.client.OpenLineage.DocumentationDatasetFacet;
import io.openlineage.client.OpenLineage.LifecycleStateChangeDatasetFacet;
import io.openlineage.client.OpenLineage.OwnershipDatasetFacet;
import io.openlineage.client.OpenLineage.SchemaDatasetFacet;
import io.openlineage.client.OpenLineage.StorageDatasetFacet;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacet;
import io.openlineage.client.OpenLineageClientUtils;
import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;

/** Utility class to load serialized facets json string into a dataset builder */
@Slf4j
class ExtesionDataSourceV2Utils {

  public static final String OPENLINEAGE_DATASET_FACETS_PREFIX = "openlineage.dataset.facets.";
  private static Map<String, TypeReference> predefinedFacets;

  static {
    predefinedFacets = new HashMap<String, TypeReference>();
    predefinedFacets.put("documentation", new TypeReference<DocumentationDatasetFacet>() {});
    predefinedFacets.put("dataSource", new TypeReference<DatasourceDatasetFacet>() {});
    predefinedFacets.put("version", new TypeReference<DatasetVersionDatasetFacet>() {});
    predefinedFacets.put("schema", new TypeReference<SchemaDatasetFacet>() {});
    predefinedFacets.put("ownership", new TypeReference<OwnershipDatasetFacet>() {});
    predefinedFacets.put("storage", new TypeReference<StorageDatasetFacet>() {});
    predefinedFacets.put("columnLineage", new TypeReference<ColumnLineageDatasetFacet>() {});
    predefinedFacets.put("symlinks", new TypeReference<SymlinksDatasetFacet>() {});
    predefinedFacets.put(
        "lifecycleStateChange", new TypeReference<LifecycleStateChangeDatasetFacet>() {});
  }

  /**
   * Given a table properties, it adds facets to builders from string representation within
   * properties
   */
  public static void loadBuilder(
      OpenLineage openLineage,
      OpenLineage.DatasetFacetsBuilder builder,
      DataSourceV2Relation relation) {
    Map<String, String> properties = relation.table().properties();

    predefinedFacets.keySet().stream()
        .filter(field -> properties.containsKey(OPENLINEAGE_DATASET_FACETS_PREFIX + field))
        .forEach(
            field -> {
              try {
                Object o =
                    OpenLineageClientUtils.fromJson(
                        properties.get(OPENLINEAGE_DATASET_FACETS_PREFIX + field),
                        predefinedFacets.get(field));
                FieldUtils.writeField(builder, field, o, true);
              } catch (IllegalAccessException | RuntimeException e) {
                log.warn("Couldn't serialize and assign facet", e);
              }
            });

    // custom facets
    properties.keySet().stream()
        .filter(key -> key.startsWith(OPENLINEAGE_DATASET_FACETS_PREFIX))
        .map(key -> StringUtils.substringAfterLast(key, "."))
        .filter(key -> !predefinedFacets.containsKey(key))
        .forEach(
            key -> {
              try {
                DatasetFacet datasetFacet =
                    OpenLineageClientUtils.fromJson(
                        properties.get(OPENLINEAGE_DATASET_FACETS_PREFIX + key),
                        new TypeReference<DatasetFacet>() {});
                builder.put(
                    key,
                    enrichWithProducerUrl(
                        datasetFacet, openLineage.newDatasetFacet().get_producer()));
              } catch (RuntimeException e) {
                log.warn("Couldn't serialize and assign facet", e);
              }
            });
  }

  private static DatasetFacet enrichWithProducerUrl(DatasetFacet facet, URI producerUri) {
    if (facet.get_producer() != null && facet.get_producer().equals(producerUri)) {
      // no need to modify anything
      return facet;
    }
    return new DatasetFacet() {
      URI _producer = producerUri;
      URI _schemaURL = facet.get_schemaURL();
      Boolean _deleted = facet.isDeleted();
      @JsonAnySetter Map<String, Object> additionalProperties = facet.getAdditionalProperties();

      @Override
      public URI get_producer() {
        return producerUri;
      }

      @Override
      public URI get_schemaURL() {
        return _schemaURL;
      }

      @Override
      public Boolean get_deleted() {
        return _deleted;
      }

      @Override
      public Map<String, Object> getAdditionalProperties() {
        return additionalProperties;
      }
    };
  }

  public static DatasetIdentifier getDatasetIdentifier(DataSourceV2Relation relation) {
    return new DatasetIdentifier(
        relation.table().properties().get("openlineage.dataset.name"),
        relation.table().properties().get("openlineage.dataset.namespace"));
  }

  public static boolean hasExtensionLineage(DataSourceV2Relation relation) {
    return Optional.ofNullable(relation)
        .map(r -> r.table())
        .map(table -> table.properties())
        .filter(Objects::nonNull)
        .filter(props -> props.containsKey("openlineage.dataset.name"))
        .filter(props -> props.containsKey("openlineage.dataset.namespace"))
        .isPresent();
  }
}
