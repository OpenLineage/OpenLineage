/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class PatternMatchingGroupNamespaceResolver implements DatasetNamespaceResolver {

  private final String matchingGroup;
  private final Pattern pattern;
  private final String schema;

  public PatternMatchingGroupNamespaceResolver(PatternMatchingGroupNamespaceResolverConfig config) {
    this.matchingGroup = config.getMatchingGroup();
    this.pattern = Pattern.compile(config.getRegex());
    this.schema = config.getSchema();
  }

  @Override
  public String resolve(String namespace) {
    if (StringUtils.isNotEmpty(schema) && !namespace.startsWith(schema + "://")) {
      // schema configured but is not matching
      return namespace;
    }

    Matcher matcher = pattern.matcher(namespace);
    if (matcher.find()) {
      return matcher.replaceAll(matcher.group(matchingGroup));
    } else {
      return namespace;
    }
  }
}
