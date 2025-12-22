/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacet;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFields;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsAdditional;
import io.openlineage.client.OpenLineage.ColumnLineageDatasetFacetFieldsBuilder;
import io.openlineage.client.OpenLineage.DatasetFacets;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputField;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;

/**
 * Utility class to handle removing path patterns in dataset names. Given a configured regex pattern
 * with "remove" group defined, class methods run regex replacements on all the datasets available
 * within the event
 */
public class RemovePathPatternUtils {
  public static final String REMOVE_PATTERN_GROUP = "remove";
  public static final String SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN =
      "spark.openlineage.dataset.removePath.pattern";

  public static List<OutputDataset> removeOutputsPathPattern(
      OpenLineageContext context, List<OutputDataset> outputs) {
    return getPattern(context)
        .map(
            pattern ->
                outputs.stream()
                    .map(
                        dataset -> {
                          String newName = removePath(pattern, dataset.getName());
                          DatasetFacets newFacets =
                              removePathFromFacets(pattern, dataset.getFacets(), context);
                          if (!Objects.equals(newName, dataset.getName())
                              || newFacets != dataset.getFacets()) {
                            return context
                                .getOpenLineage()
                                .newOutputDatasetBuilder()
                                .name(newName)
                                .namespace(dataset.getNamespace())
                                .facets(newFacets)
                                .outputFacets(dataset.getOutputFacets())
                                .build();
                          } else {
                            return dataset;
                          }
                        })
                    .collect(Collectors.toList()))
        .orElse(outputs);
  }

  public static List<InputDataset> removeInputsPathPattern(
      OpenLineageContext context, List<InputDataset> outputs) {
    return getPattern(context)
        .map(
            pattern ->
                outputs.stream()
                    .map(
                        dataset -> {
                          String newName = removePath(pattern, dataset.getName());
                          if (!Objects.equals(newName, dataset.getName())) {
                            return context
                                .getOpenLineage()
                                .newInputDatasetBuilder()
                                .name(newName)
                                .namespace(dataset.getNamespace())
                                .facets(dataset.getFacets())
                                .inputFacets(dataset.getInputFacets())
                                .build();
                          } else {
                            return dataset;
                          }
                        })
                    .collect(Collectors.toList()))
        .orElse(outputs);
  }

  private static DatasetFacets removePathFromFacets(
      Pattern pattern, DatasetFacets facets, OpenLineageContext context) {
    if (facets == null || facets.getColumnLineage() == null) {
      return facets;
    }

    ColumnLineageDatasetFacet originalFacet = facets.getColumnLineage();
    List<InputField> newDataset = processInputFields(pattern, originalFacet.getDataset(), context);
    ColumnLineageDatasetFacetFields newFields =
        processFieldMappings(pattern, originalFacet.getFields(), context);

    if (newDataset != originalFacet.getDataset() || newFields != originalFacet.getFields()) {
      return DatasetFacetsUtils.copyToBuilder(context, facets)
          .columnLineage(
              context.getOpenLineage().newColumnLineageDatasetFacet(newFields, newDataset))
          .build();
    }
    return facets;
  }

  private static List<InputField> processInputFields(
      Pattern pattern, List<InputField> inputFields, OpenLineageContext context) {
    if (inputFields == null || inputFields.isEmpty()) {
      return inputFields;
    }

    List<InputField> processedFields = new ArrayList<>();
    boolean hasChanges = false;

    for (InputField field : inputFields) {
      String processedName = removePath(pattern, field.getName());
      if (!processedName.equals(field.getName())) {
        processedFields.add(
            context
                .getOpenLineage()
                .newInputFieldBuilder()
                .namespace(field.getNamespace())
                .name(processedName)
                .field(field.getField())
                .transformations(field.getTransformations())
                .build());
        hasChanges = true;
      } else {
        processedFields.add(field);
      }
    }
    return hasChanges ? processedFields : inputFields;
  }

  private static ColumnLineageDatasetFacetFields processFieldMappings(
      Pattern pattern, ColumnLineageDatasetFacetFields fields, OpenLineageContext context) {
    if (fields == null || fields.getAdditionalProperties().isEmpty()) {
      return fields;
    }

    ColumnLineageDatasetFacetFieldsBuilder builder =
        context.getOpenLineage().newColumnLineageDatasetFacetFieldsBuilder();
    boolean hasChanges = false;

    for (Map.Entry<String, ColumnLineageDatasetFacetFieldsAdditional> entry :
        fields.getAdditionalProperties().entrySet()) {
      ColumnLineageDatasetFacetFieldsAdditional original = entry.getValue();
      List<InputField> processedInputFields =
          processInputFields(pattern, original.getInputFields(), context);

      if (processedInputFields != original.getInputFields()) {
        builder.put(
            entry.getKey(),
            context
                .getOpenLineage()
                .newColumnLineageDatasetFacetFieldsAdditional(
                    processedInputFields,
                    original.getTransformationDescription(),
                    original.getTransformationType()));
        hasChanges = true;
      } else {
        builder.put(entry.getKey(), original);
      }
    }
    return hasChanges ? builder.build() : fields;
  }

  private static Optional<Pattern> getPattern(OpenLineageContext context) {
    return context
        .getSparkContext()
        .map(sparkContext -> sparkContext.conf())
        .filter(conf -> conf.contains(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .map(conf -> conf.get(SPARK_OPENLINEAGE_DATASET_REMOVE_PATH_PATTERN))
        .map(pattern -> Pattern.compile(pattern));
  }

  private static String removePath(Pattern pattern, String name) {
    return Optional.ofNullable(pattern.matcher(name))
        .filter(matcher -> matcher.find())
        .filter(
            matcher -> {
              try {
                matcher.group(REMOVE_PATTERN_GROUP);
                return true;
              } catch (IllegalStateException | IllegalArgumentException e) {
                return false;
              }
            })
        .filter(matcher -> StringUtils.isNotEmpty(matcher.group(REMOVE_PATTERN_GROUP)))
        .map(
            matcher ->
                name.substring(0, matcher.start(REMOVE_PATTERN_GROUP))
                    + name.substring(matcher.end(REMOVE_PATTERN_GROUP), name.length()))
        .orElse(name);
  }
}
