/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.spark3.agent.utils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/** Parses {@code COPY INTO} statements when tahoe command reflection is unavailable. */
public class CopyIntoSqlUtils {

  private static final Pattern COPY_INTO_TARGET =
      Pattern.compile("(?is)\\bCOPY\\s+INTO\\s+([`\\w.]+)");
  private static final Pattern COPY_INTO_SOURCE =
      Pattern.compile("(?is)\\bFROM\\s+'([^']+)'|\\bFROM\\s+\"([^\"]+)\"");

  private CopyIntoSqlUtils() {}

  public static boolean isCopyIntoStatement(String sql) {
    return sql != null && COPY_INTO_TARGET.matcher(sql).find();
  }

  public static Optional<String> targetTable(String sql) {
    if (StringUtils.isBlank(sql)) {
      return Optional.empty();
    }
    Matcher matcher = COPY_INTO_TARGET.matcher(sql);
    if (!matcher.find()) {
      return Optional.empty();
    }
    return Optional.of(stripQuotes(matcher.group(1)));
  }

  public static Optional<String> sourcePath(String sql) {
    if (StringUtils.isBlank(sql)) {
      return Optional.empty();
    }
    Matcher matcher = COPY_INTO_SOURCE.matcher(sql);
    if (!matcher.find()) {
      return Optional.empty();
    }
    String singleQuoted = matcher.group(1);
    String doubleQuoted = matcher.group(2);
    return Optional.ofNullable(StringUtils.firstNonBlank(singleQuoted, doubleQuoted))
        .filter(StringUtils::isNotBlank);
  }

  private static String stripQuotes(String value) {
    return value.replace("`", "").trim();
  }
}
