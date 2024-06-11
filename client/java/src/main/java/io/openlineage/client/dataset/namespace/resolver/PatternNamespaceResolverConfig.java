/*
/* Copyright 2018-2024 contributors to the OpenLineage project
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
public class PatternNamespaceResolverConfig
    implements DatasetNamespaceResolverConfig, MergeConfig<PatternNamespaceResolverConfig> {

  @NonNull @Getter @Setter private String regex;
  @Getter @Setter private String schema;

  @Override
  public PatternNamespaceResolverConfig mergeWithNonNull(PatternNamespaceResolverConfig t) {
    return new PatternNamespaceResolverConfig(
        mergePropertyWith(regex, t.regex), mergePropertyWith(schema, t.schema));
  }
}
