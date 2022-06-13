/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import lombok.Value;

import java.net.URI;

public class DatasetParser {
  public static DatasetParseResult parse(URI uri) {
    return new DatasetParseResult(getName(uri), getNamespace(uri));
  }

  private static String getName(URI uri) {
    return uri.toASCIIString();
  }

  private static String getNamespace(URI uri) {
    if (isNullOrEmpty(uri.getScheme()) && isNullOrEmpty(uri.getHost())) {
      return "default";
    } else if (isNullOrEmpty(uri.getHost())) {
      return uri.getScheme();
    } else if (isNullOrEmpty(uri.getScheme())) {
      return uri.getHost();
    }
    return String.format("%s.%s", uri.getScheme(), uri.getHost());
  }

  private static boolean isNullOrEmpty(String str) {
    return str == null || str.isEmpty();
  }

  public static DatasetParseResult parse(String value) {
    try {
      URI uri = URI.create(value);
      return parse(uri);
    } catch (Exception e) {
      return new DatasetParseResult(value, "default");
    }
  }

  @Value
  public static class DatasetParseResult {
    String name;
    String namespace;
  }
}
