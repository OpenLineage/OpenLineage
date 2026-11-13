/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.sqlagent;

import io.openlineage.client.OpenLineage;
import io.openlineage.flink.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Converts captured SQL datasets to the official OpenLineage Java model. */
final class SqlOpenLineageConverter {
  private final OpenLineageContext context;
  private final OpenLineage ol;

  SqlOpenLineageConverter(OpenLineageContext context) {
    this.context = context;
    this.ol = context.getOpenLineage();
  }

  List<OpenLineage.InputDataset> inputs(Map<String, Object> snapshot) {
    List<OpenLineage.InputDataset> result = new ArrayList<>();
    for (Map<String, Object> dataset : maps(snapshot.get("inputs"))) {
      OpenLineage.DatasetFacetsBuilder facets =
          ol.newDatasetFacetsBuilder().schema(schema(dataset));
      dataSource(dataset).ifPresent(facets::dataSource);
      result.add(
          ol.newInputDatasetBuilder()
              .namespace(namespace(dataset))
              .name((String) dataset.get("name"))
              .facets(facets.build())
              .build());
    }
    return Collections.unmodifiableList(result);
  }

  List<OpenLineage.OutputDataset> outputs(Map<String, Object> snapshot) {
    List<OpenLineage.OutputDataset> result = new ArrayList<>();
    for (Map<String, Object> dataset : maps(snapshot.get("outputs"))) {
      Map<String, Object> facets = map(dataset.get("facets"));
      OpenLineage.DatasetFacetsBuilder builder =
          ol.newDatasetFacetsBuilder().schema(schema(dataset));
      dataSource(dataset).ifPresent(builder::dataSource);
      if (facets.containsKey("columnLineage")) {
        builder.columnLineage(columnLineage(map(facets.get("columnLineage"))));
      }
      result.add(
          ol.newOutputDatasetBuilder()
              .namespace(namespace(dataset))
              .name((String) dataset.get("name"))
              .facets(builder.build())
              .build());
    }
    return Collections.unmodifiableList(result);
  }

  private OpenLineage.SchemaDatasetFacet schema(Map<String, Object> dataset) {
    Map<String, Object> schema = map(map(dataset.get("facets")).get("schema"));
    List<OpenLineage.SchemaDatasetFacetFields> fields = new ArrayList<>();
    for (Map<String, Object> field : maps(schema.get("fields"))) {
      fields.add(
          ol.newSchemaDatasetFacetFieldsBuilder()
              .name((String) field.get("name"))
              .type((String) field.get("type"))
              .build());
    }
    return ol.newSchemaDatasetFacet(fields);
  }

  private java.util.Optional<OpenLineage.DatasourceDatasetFacet> dataSource(
      Map<String, Object> dataset) {
    Object value = dataset.get("dataSourceUri");
    if (!(value instanceof String)) return java.util.Optional.empty();
    String uri = (String) value;
    return java.util.Optional.of(
        ol.newDatasourceDatasetFacetBuilder().name(uri).uri(java.net.URI.create(uri)).build());
  }

  private OpenLineage.ColumnLineageDatasetFacet columnLineage(Map<String, Object> facet) {
    OpenLineage.ColumnLineageDatasetFacetFieldsBuilder fields =
        ol.newColumnLineageDatasetFacetFieldsBuilder();
    for (Map.Entry<String, Object> entry : map(facet.get("fields")).entrySet()) {
      List<OpenLineage.InputField> refs =
          inputFields(maps(map(entry.getValue()).get("inputFields")));
      fields.put(
          entry.getKey(),
          ol.newColumnLineageDatasetFacetFieldsAdditionalBuilder().inputFields(refs).build());
    }
    return ol.newColumnLineageDatasetFacetBuilder()
        .fields(fields.build())
        .dataset(inputFields(maps(facet.get("dataset"))))
        .build();
  }

  private List<OpenLineage.InputField> inputFields(List<Map<String, Object>> references) {
    List<OpenLineage.InputField> result = new ArrayList<>();
    for (Map<String, Object> reference : references) {
      List<OpenLineage.InputFieldTransformations> transformations = new ArrayList<>();
      for (Map<String, Object> transformation : maps(reference.get("transformations"))) {
        OpenLineage.InputFieldTransformationsBuilder builder =
            ol.newInputFieldTransformationsBuilder()
                .type((String) transformation.get("type"))
                .subtype((String) transformation.get("subtype"));
        if (transformation.containsKey("description")) {
          builder.description((String) transformation.get("description"));
        }
        transformations.add(builder.build());
      }
      result.add(
          ol.newInputFieldBuilder()
              .namespace(
                  context.getNamespaceResolver().resolve((String) reference.get("namespace")))
              .name((String) reference.get("name"))
              .field((String) reference.get("field"))
              .transformations(transformations)
              .build());
    }
    return result;
  }

  private String namespace(Map<String, Object> dataset) {
    return context.getNamespaceResolver().resolve((String) dataset.get("namespace"));
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> map(Object value) {
    return value == null ? Collections.emptyMap() : (Map<String, Object>) value;
  }

  @SuppressWarnings("unchecked")
  private static List<Map<String, Object>> maps(Object value) {
    return value == null ? Collections.emptyList() : (List<Map<String, Object>>) value;
  }
}
