/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.util;

import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.spark.api.OpenLineageContext;
import java.util.List;
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
                          if (newName != dataset.getName()) {
                            return context
                                .getOpenLineage()
                                .newOutputDatasetBuilder()
                                .name(removePath(pattern, dataset.getName()))
                                .namespace(dataset.getNamespace())
                                .facets(dataset.getFacets())
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
                          if (newName != dataset.getName()) {
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
