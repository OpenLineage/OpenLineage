/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.dataset.namespace.resolver;

import io.openlineage.client.MergeConfig;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class PatternMatchingGroupNamespaceResolverConfig
    implements DatasetNamespaceResolverConfig,
        MergeConfig<PatternMatchingGroupNamespaceResolverConfig> {

  @NonNull @Getter @Setter private String regex;
  @NonNull @Getter @Setter private String matchingGroup;
  @Getter @Setter private String schema;

  @Override
  public PatternMatchingGroupNamespaceResolverConfig mergeWithNonNull(
      PatternMatchingGroupNamespaceResolverConfig t) {
    return new PatternMatchingGroupNamespaceResolverConfig(
        mergePropertyWith(regex, t.regex),
        mergePropertyWith(matchingGroup, t.matchingGroup),
        mergePropertyWith(schema, t.schema));
  }
}
