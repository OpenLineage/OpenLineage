/*
/* Copyright 2018-2022 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.flink.utils;

import io.openlineage.flink.api.DatasetIdentifier;
import java.net.URI;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class PathUtils {

  private static final String DEFAULT_SCHEME = "file";

  public static DatasetIdentifier fromURI(URI location) {
    return PathUtils.fromURI(location, DEFAULT_SCHEME);
  }

  public static DatasetIdentifier fromURI(URI location, String defaultScheme) {
    if (location.isAbsolute() && location.getAuthority() == null && location.getScheme() == null) {
      return new DatasetIdentifier(location.toString(), defaultScheme);
    }
    String namespace =
        Optional.ofNullable(location.getAuthority())
            .map(a -> String.format("%s://%s", location.getScheme(), a))
            .orElseGet(() -> (location.getScheme() != null) ? location.getScheme() : defaultScheme);
    String name = trimSlashesInName(location.getPath());
    return new DatasetIdentifier(name, namespace);
  }

  static String trimSlashesInName(String name) {
    return StringUtils.removeEnd(StringUtils.removeStart(name, "/"), "/");
  }
}
