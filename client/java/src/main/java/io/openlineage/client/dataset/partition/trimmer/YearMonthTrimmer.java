/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.partition.trimmer;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.List;

/**
 * Normalizes if last part is a string representing a year month in an arbitrary format.
 *
 * <p>Heuristics: - try to find within a string a year month in any of the defined formats: yyyyMM,
 * yyyy-MM - if not found, return false - remove the identified year month from the string
 */
public class YearMonthTrimmer implements DatasetNameTrimmer {

  // Common YearMonth formats
  private static final List<DateTimeFormatter> FORMATTERS =
      Arrays.asList(
          DateTimeFormatter.ofPattern("yyyyMM"), // e.g. 202501
          DateTimeFormatter.ofPattern("yyyy-MM") // e.g. 2025-01
          );

  @Override
  public boolean canTrim(String name) {
    if (!hasMultipleDirectories(name)) {
      return false;
    }
    String lastPart = getLastPart(name);
    for (DateTimeFormatter formatter : FORMATTERS) {
      try {
        // parseStrict ensures invalid dates (like 2025-13) fail
        YearMonth.parse(lastPart, formatter);
        return true;
      } catch (DateTimeParseException e) {
        // do nothing, try next formatter
      }
    }
    return false;
  }
}
