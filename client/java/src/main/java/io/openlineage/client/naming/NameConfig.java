/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.naming;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * Configuration for OpenLineage name-related behaviour.
 *
 * <p>This class is bound to the {@code name} key in the top-level OpenLineage configuration:
 *
 * <pre>{@code
 * name:
 *   escaping: false   # disable automatic dot-escaping of name segments
 * }</pre>
 *
 * <p>The same setting can also be applied through the dynamic environment variable convention:
 *
 * <pre>{@code
 * OPENLINEAGE__NAME__ESCAPING=false
 * }</pre>
 *
 * <p>When the environment variable is set it takes precedence because {@link NameEscaping} reads
 * {@code System.getenv("OPENLINEAGE__NAME__ESCAPING")} at call time, independently of whether the
 * YAML configuration has been loaded.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NameConfig implements MergeConfig<NameConfig> {

  /**
   * When {@code false}, automatic dot-escaping of name segments is disabled. Defaults to {@code
   * null}, which is treated as {@code true} (escaping enabled) by {@link NameEscaping}.
   */
  @JsonProperty("escaping")
  private Boolean escaping;

  @Override
  public NameConfig mergeWithNonNull(NameConfig other) {
    NameConfig merged = new NameConfig();
    merged.escaping = mergePropertyWith(this.escaping, other.escaping);
    return merged;
  }
}
