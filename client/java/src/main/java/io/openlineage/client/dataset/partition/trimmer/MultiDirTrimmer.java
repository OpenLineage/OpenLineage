/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.client.dataset.partition.trimmer;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

/**
 * Trims directory if last path is a string represents a date in a format /yyyy/MM or /yyyy/MM/dd
 */
public class MultiDirTrimmer implements DatasetNameTrimmer {

  DateTimeFormatter yearMonthFormatter = DateTimeFormatter.ofPattern("yyyyMM");

  DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyyMMdd");

  @Override
  public boolean canTrim(String name) {
    String[] dirs = name.split(DatasetNameTrimmer.SEPARATOR);
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
    if (dirs.length < 2) {
      return false;
    }

    // get two last paths and verify if they're a year month
    String lastTwoPaths = dirs[dirs.length - 2] + dirs[dirs.length - 1];
    try {
      YearMonth.parse(lastTwoPaths, yearMonthFormatter);
      return true;
    } catch (DateTimeParseException e) {
      // do nothing
    }

    return false;
  }

  @SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
  private boolean isMultiDirDate(String... dirs) {
    if (dirs.length < 3) {
      return false;
    }

    // get three last paths and verify if they're a date
    String lastTwoPaths = dirs[dirs.length - 3] + dirs[dirs.length - 2] + dirs[dirs.length - 1];
    try {
      LocalDate.parse(lastTwoPaths, dateFormatter);
      return true;
    } catch (DateTimeParseException e) {
      // do nothing
    }

    return false;
  }
}
