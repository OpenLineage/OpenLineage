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
      Pattern.compile(
          "(?is)\\bCOPY\\s+INTO\\s+"
              + "((?:`(?:``|[^`])+`|[\\w-]+)"
              + "(?:\\.(?:`(?:``|[^`])+`|[\\w-]+))*)");
  private static final Pattern COPY_INTO_SOURCE =
      Pattern.compile("(?is)\\bFROM\\s+'([^']+)'|\\bFROM\\s+\"([^\"]+)\"");
  private static final Pattern COPY_INTO_VALIDATE =
      Pattern.compile(
          "(?is)\\bFILEFORMAT\\s*=\\s*[A-Z][A-Z0-9_]*"
              + "\\s+VALIDATE\\b"
              + "(?:\\s+(?:ALL|[1-9]\\d*\\s+ROWS))?"
              + "(?=\\s*(?:FILES\\s*=|PATTERN\\s*=|FORMAT_OPTIONS\\s*\\(|"
              + "COPY_OPTIONS\\s*\\(|;|$))");

  private CopyIntoSqlUtils() {}

  public static boolean isCopyIntoStatement(String sql) {
    return sql != null && COPY_INTO_TARGET.matcher(sql).find();
  }

  /** Returns true when the statement validates source data without writing to the target table. */
  public static boolean isValidateStatement(String sql) {
    return isCopyIntoStatement(sql) && COPY_INTO_VALIDATE.matcher(sql).find();
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
    if (StringUtils.isBlank(value)) {
      return value;
    }
    String[] parts = value.split("\\.", -1);
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < parts.length; i++) {
      if (i > 0) {
        result.append('.');
      }
      result.append(unescapeBacktickIdentifier(parts[i]));
    }
    return result.toString().trim();
  }

  private static String unescapeBacktickIdentifier(String part) {
    if (part.startsWith("`") && part.endsWith("`") && part.length() >= 2) {
      return part.substring(1, part.length() - 1).replace("``", "`");
    }
    return part;
  }
}
