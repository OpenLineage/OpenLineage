/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils.jdbc;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Minimal parser for Oracle TNS connect descriptors, e.g. {@code
 * (DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)(HOST=h)(PORT=1521))(CONNECT_DATA=(SERVICE_NAME=s)))}.
 *
 * <p>Only extracts the pieces needed to build a JDBC-style namespace/name: HOST+PORT of every
 * ADDRESS node (wherever nested, e.g. directly under DESCRIPTION or under an ADDRESS_LIST), and
 * SERVICE_NAME/SID/INSTANCE_NAME from CONNECT_DATA.
 */
final class TnsDescriptor {

  private final String key;
  private String value;
  private final List<TnsDescriptor> children = new ArrayList<>();

  private TnsDescriptor(String key) {
    this.key = key;
  }

  static TnsDescriptor parse(String s) throws URISyntaxException {
    int[] pos = {0};
    skipWs(s, pos);
    if (pos[0] >= s.length() || s.charAt(pos[0]) != '(') {
      throw new URISyntaxException(s, "Expected TNS descriptor to start with '('");
    }
    TnsDescriptor root = parseNode(s, pos);
    skipWs(s, pos);
    if (pos[0] < s.length()) {
      // Trailing unbalanced/garbage content after the root group - treat the whole
      // descriptor as malformed rather than silently returning a partial parse.
      throw new URISyntaxException(s, "Unexpected trailing content after TNS descriptor");
    }
    return root;
  }

  // pos is a single-element mutable cursor passed by reference, not a variable-length
  // argument list, so varargs would be the wrong fit here.
  @SuppressWarnings("PMD.UseVarargs")
  private static TnsDescriptor parseNode(String s, int[] pos) throws URISyntaxException {
    expect(s, pos, '(');
    skipWs(s, pos);
    int keyStart = pos[0];
    while (pos[0] < s.length() && s.charAt(pos[0]) != '=' && s.charAt(pos[0]) != ')') {
      pos[0]++;
    }
    String key = s.substring(keyStart, pos[0]).trim().toUpperCase(Locale.ROOT);
    TnsDescriptor node = new TnsDescriptor(key);

    if (pos[0] < s.length() && s.charAt(pos[0]) == '=') {
      pos[0]++; // consume '='
      skipWs(s, pos);
      if (pos[0] < s.length() && s.charAt(pos[0]) == '(') {
        while (pos[0] < s.length() && s.charAt(pos[0]) == '(') {
          node.children.add(parseNode(s, pos));
          skipWs(s, pos);
        }
      } else {
        int valStart = pos[0];
        while (pos[0] < s.length() && s.charAt(pos[0]) != ')') {
          pos[0]++;
        }
        node.value = s.substring(valStart, pos[0]).trim();
      }
    }
    skipWs(s, pos);
    expect(s, pos, ')');
    return node;
  }

  private static void expect(String s, int[] pos, char c) throws URISyntaxException {
    if (pos[0] >= s.length() || s.charAt(pos[0]) != c) {
      throw new URISyntaxException(s, "Expected '" + c + "' at position " + pos[0]);
    }
    pos[0]++;
  }

  @SuppressWarnings("PMD.UseVarargs")
  private static void skipWs(String s, int[] pos) {
    while (pos[0] < s.length() && Character.isWhitespace(s.charAt(pos[0]))) {
      pos[0]++;
    }
  }

  /** Recursively collects every descendant (including self) with the given key. */
  List<TnsDescriptor> findAll(String wantedKey) {
    List<TnsDescriptor> result = new ArrayList<>();
    if (this.key.equals(wantedKey)) {
      result.add(this);
    }
    for (TnsDescriptor child : children) {
      result.addAll(child.findAll(wantedKey));
    }
    return result;
  }

  /** Direct-child lookup, case-insensitive on already-uppercased keys. */
  Optional<String> childValue(String wantedKey) {
    return children.stream()
        .filter(c -> c.key.equals(wantedKey))
        .map(c -> c.value)
        .filter(v -> v != null)
        .findFirst();
  }
}
