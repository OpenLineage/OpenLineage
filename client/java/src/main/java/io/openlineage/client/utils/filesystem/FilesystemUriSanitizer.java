/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.filesystem;

import java.net.URI;
import lombok.SneakyThrows;
import org.apache.commons.lang3.StringUtils;

class FilesystemUriSanitizer {
  /**
   * Convert URI from `/path` to `$defaultScheme:/path`
   *
   * @param uri filesystem URI
   * @param defaultScheme default scheme
   * @return URI
   */
  @SneakyThrows
  public static URI applyScheme(URI uri, String defaultScheme) {
    String scheme = StringUtils.defaultIfEmpty(uri.getScheme(), defaultScheme);
    return new URI(scheme, uri.getAuthority(), uri.getPath(), null, null);
  }

  /**
   * Convert `file:/some/path/` to `file:/some/path`
   *
   * @param uri filesystem URI
   * @param defaultScheme default scheme
   * @return String
   */
  public static String removeFirstSlash(String uri) {
    return StringUtils.stripStart(uri, "/");
  }

  /**
   * Convert `file:/some/path/` to `file:/some/path`
   *
   * @param uri filesystem URI
   * @param defaultScheme default scheme
   * @return String
   */
  public static String removeLastSlash(String uri) {
    return StringUtils.stripEnd(uri, "/");
  }

  /**
   * Replace empty path string with `/`
   *
   * @param path filesystem path
   * @return String
   */
  public static String nonEmptyPath(String path) {
    return StringUtils.defaultIfEmpty(path, "/");
  }
}
