/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class PatternNamespaceResolver implements DatasetNamespaceResolver {
  private final String resolvedName;
  private final Pattern pattern;
  private final String schema;

  public PatternNamespaceResolver(String resolvedName, PatternNamespaceResolverConfig config) {
    this.resolvedName = resolvedName;
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
      return matcher.replaceAll(resolvedName);
    } else {
      return namespace;
    }
  }
}
