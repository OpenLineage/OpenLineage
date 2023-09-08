/*
/* Copyright 2018-2023 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import java.net.URI;
import java.util.Optional;
import java.util.regex.Pattern;

public class DatasetIdentifierUtils {

  private static final String DEFAULT_SCHEME = "file";
  /** The directory separator, a slash, as a character. */
  public static final char SEPARATOR_CHAR = '/';
  /** The directory separator, a slash. */
  public static final String SEPARATOR = "/";

  /** Whether the current host is a Windows machine. */
  public static final boolean WINDOWS = System.getProperty("os.name").startsWith("Windows");

  /** Pre-org.apache.hadoop.shaded.com.iled regular expressions to detect path formats. */
  private static final Pattern HAS_DRIVE_LETTER_SPECIFIER = Pattern.compile("^/?[a-zA-Z]:");

  public static DatasetIdentifier fromURI(URI uri) {
    return fromURI(uri, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromURI(URI uri, String defaultScheme) {
    if (isAbsoluteAndSchemeAuthorityNull(uri)) {
      return new DatasetIdentifier(uri.getPath(), defaultScheme);
    }

    String name =
        Optional.of(uri.getPath())
            .map(DatasetIdentifierUtils::removeLastSlash)
            .map(DatasetIdentifierUtils::removeFirstSlashIfSingleSlashInString)
            .get();

    String namespace =
        Optional.ofNullable(uri.getAuthority())
            .map(a -> String.format("%s://%s", uri.getScheme(), a))
            .orElseGet(() -> (uri.getScheme() != null) ? uri.getScheme() : defaultScheme);

    return new DatasetIdentifier(name, namespace);
  }

  private static String removeFirstSlashIfSingleSlashInString(String name) {
    if (name.chars().filter(x -> x == '/').count() == 1 && name.startsWith("/")) {
      return name.substring(1);
    }
    return name;
  }

  private static String removeLastSlash(String name) {
    if (name.charAt(name.length() - 1) == '/') {
      return name.substring(0, name.length() - 1);
    }
    return name;
  }

  /**
   * Copied implementation of `isAbsoluteAndSchemeAuthorityNull` method in Path class within hadoop
   * common package. We don't want to add 4MB dependency, however we need to have a method that
   * checks if a path is absolute in that way.
   *
   * @see
   *     <a=href"https://github.com/apache/hadoop/blob/release-3.3.2-RC0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/fs/Path.java#L376">original
   *     method</a>
   */
  private static boolean isAbsoluteAndSchemeAuthorityNull(URI uri) {
    if (uri.getScheme() != null || uri.getAuthority() != null) {
      return false;
    }

    boolean hasWindowsDrive = (WINDOWS && HAS_DRIVE_LETTER_SPECIFIER.matcher(uri.getPath()).find());

    int startPositionWithoutWindowsDrive = 0;
    if (hasWindowsDrive) {
      startPositionWithoutWindowsDrive = (uri.getPath().charAt(0) == SEPARATOR_CHAR ? 3 : 2);
    }

    return uri.getPath().startsWith(SEPARATOR, startPositionWithoutWindowsDrive);
  }
}
