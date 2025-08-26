/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.partition.trimmer;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Normalizes if last path is a string represents a data in an arbitrary format.
 *
 * <p>Heuristics: - try to find within a string a date in any of the defined formats: yyyy-MM-dd,
 * dd.MM.yyyy, yyyyMMdd - if not found, return false - remove the identified date from the string -
 * remove single characters 'T' and 'Z' if they are present - remove whitespaces, colons, dots and
 * hyphens - everything else should be empty of numeric characters
 */
public class DateTrimmer implements DatasetNameTrimmer {

  private static final String[][] DATE_FORMATS_AND_REGEX = {
    {"yyyy-MM-dd", "\\d{4}-\\d{2}-\\d{2}"},
    {"dd.MM.yyyy", "\\d{2}\\.\\d{2}\\.\\d{4}"},
    {"yyyyMMdd", "\\d{8}"}
  };

  private static final Map<String, Pattern> DATE_FORMATS_AND_REGEX_MAP =
      Arrays.stream(DATE_FORMATS_AND_REGEX)
          .collect(Collectors.toMap(pair -> pair[0], pair -> Pattern.compile(pair[1])));

  @Override
  public boolean canTrim(String name) {
    String lastDir = getLastPath(name);
    for (Entry<String, Pattern> entry : DATE_FORMATS_AND_REGEX_MAP.entrySet()) {
      String format = entry.getKey();
      Pattern pattern = entry.getValue();
      Matcher matcher = pattern.matcher(lastDir);

      // find all pattern matches
      while (matcher.find()) {
        String candidate = matcher.group();
        try {
          // examine if a pattern match is a valid date
          DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
          LocalDate.parse(candidate, formatter);

          // date has been found
          return lastDir
              .replaceFirst(candidate, "")
              .replaceFirst("T", "")
              .replaceFirst("Z", "")
              .replaceAll("\\s", "")
              .replaceAll(":", "")
              .replaceAll("\\.", "")
              .replaceAll("-", "")
              .replaceAll("[\\d]*", "")
              .isEmpty();
        } catch (DateTimeParseException e) {
          // ignore
        }
      }
    }
    return false; // nothing found
  }
}
