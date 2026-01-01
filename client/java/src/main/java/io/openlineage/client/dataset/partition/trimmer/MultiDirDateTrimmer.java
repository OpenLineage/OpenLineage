/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition.trimmer;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

/**
 * Trims directory if last part is a string representing a date in a format /yyyy/MM or /yyyy/MM/dd
 */
public class MultiDirDateTrimmer implements DatasetNameTrimmer {

  DateTimeFormatter yearMonthFormatter = DateTimeFormatter.ofPattern("yyyyMM");

  DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

  @Override
  public boolean canTrim(String name) {
    String[] dirs;
    if (name.startsWith("/")) {
      dirs = name.substring(1).split(SEPARATOR);
    } else {
      dirs = name.split(DatasetNameTrimmer.SEPARATOR);
    }
    return isMultiDirYearMonth(dirs) || isMultiDirDate(dirs);
  }

  /**
   * Needs to override trim method to remove more than one directory at a time
   *
   * @param name
   * @return
   */
  @Override
  public String trim(String name) {
    String[] dirs = name.split(DatasetNameTrimmer.SEPARATOR);

    if (isMultiDirYearMonth(dirs)) {
      return String.join(
          DatasetNameTrimmer.SEPARATOR, Arrays.copyOfRange(dirs, 0, dirs.length - 2));
    }

    if (isMultiDirDate(dirs)) {
      return String.join(
          DatasetNameTrimmer.SEPARATOR, Arrays.copyOfRange(dirs, 0, dirs.length - 3));
    }

    return name;
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private boolean isMultiDirYearMonth(String... dirs) {
    if (dirs.length < 3) {
      return false;
    }

    // get two last parts and verify if they're a year month
    String lastTwoParts = dirs[dirs.length - 2] + dirs[dirs.length - 1];
    try {
      YearMonth.parse(lastTwoParts, yearMonthFormatter);
      return true;
    } catch (DateTimeParseException e) {
      // do nothing
    }

    return false;
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private boolean isMultiDirDate(String... dirs) {
    if (dirs.length < 4) {
      return false;
    }

    // get three last parts and verify if they're a date
    String lastThreeParts = dirs[dirs.length - 3] + dirs[dirs.length - 2] + dirs[dirs.length - 1];
    try {
      LocalDate.parse(lastThreeParts, dateFormatter);
      return true;
    } catch (DateTimeParseException e) {
      // do nothing
    }

    return false;
  }
}
