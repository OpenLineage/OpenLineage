/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark.agent.facets.builder;

import io.openlineage.spark.agent.facets.EnvironmentFacet;
import io.openlineage.spark.api.CustomFacetBuilder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.scheduler.SparkListenerJobStart;

/**
 * {@link CustomFacetBuilder} that generates a {@link EnvironmentFacet} when using OpenLineage on
 * Databricks.
 */
@Slf4j
public class CustomEnvironmentFacetBuilder
    extends CustomFacetBuilder<SparkListenerJobStart, EnvironmentFacet> {
  private Map<String, Object> envProperties;

  public static boolean isCustomEnvironmentVarCollectionEnabled() {
    return System.getenv().containsKey("CUSTOM_VARS");
  }

  @Override
  protected void build(
      SparkListenerJobStart event, BiConsumer<String, ? super EnvironmentFacet> consumer) {
    consumer.accept(
        "environment-properties", new EnvironmentFacet(getCustomEnvironmentalAttributes(event)));
  }

  public static String toCamelCase(
      String str, final boolean capitalizeFirstLetter, final char... delimiters) {
    if (StringUtils.isEmpty(str)) {
      return str;
    }
    str = str.toLowerCase();
    final int strLen = str.length();
    final int[] newCodePoints = new int[strLen];
    int outOffset = 0;
    final Set<Integer> delimiterSet = toDelimiterSet(delimiters);
    boolean capitalizeNext = capitalizeFirstLetter;
    for (int index = 0; index < strLen; ) {
      final int codePoint = str.codePointAt(index);

      if (delimiterSet.contains(codePoint)) {
        capitalizeNext = outOffset != 0;
        index += Character.charCount(codePoint);
      } else if (capitalizeNext || outOffset == 0 && capitalizeFirstLetter) {
        final int titleCaseCodePoint = Character.toTitleCase(codePoint);
        newCodePoints[outOffset++] = titleCaseCodePoint;
        index += Character.charCount(titleCaseCodePoint);
        capitalizeNext = false;
      } else {
        newCodePoints[outOffset++] = codePoint;
        index += Character.charCount(codePoint);
      }
    }

    return new String(newCodePoints, 0, outOffset);
  }

  /**
   * Converts an array of delimiters to a hash set of code points. Code point of space(32) is added
   * as the default value. The generated hash set provides O(1) lookup time.
   *
   * @param delimiters set of characters to determine capitalization, null means whitespace
   * @return Set<Integer>
   */
  private static Set<Integer> toDelimiterSet(final char[] delimiters) {
    final Set<Integer> delimiterHashSet = new HashSet<>();
    delimiterHashSet.add(Character.codePointAt(new char[] {' '}, 0));
    if (ArrayUtils.isEmpty(delimiters)) {
      return delimiterHashSet;
    }

    for (int index = 0; index < delimiters.length; index++) {
      delimiterHashSet.add(Character.codePointAt(delimiters, index));
    }
    return delimiterHashSet;
  }

  private Map<String, Object> getCustomEnvironmentalAttributes(SparkListenerJobStart jobStart) {
    envProperties = new HashMap<>();
    // These are useful properties to extract if they are available
    String customVars = System.getenv().get("CUSTOM_VARS");
    List<String> customPropertiesToExtract = Arrays.asList(customVars.split(","));

    customPropertiesToExtract.stream()
        .forEach(
            (p) -> {
              envProperties.put(toCamelCase(p, false, new char[] {'_'}), System.getenv().get(p));
            });

    log.debug("CustomEnvProperties here: {}", envProperties.toString());
    return envProperties;
  }
}
