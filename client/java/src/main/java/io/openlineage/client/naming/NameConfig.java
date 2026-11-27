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
 *   escaping: true   # enable automatic dot-escaping of name segments (off by default)
 * }</pre>
 *
 * <p>The same setting can also be applied through the dynamic environment variable convention:
 *
 * <pre>{@code
 * OPENLINEAGE__NAME__ESCAPING=true
 * }</pre>
 *
 * <p>When both are provided, the YAML configuration takes precedence. When {@link
 * io.openlineage.client.OpenLineageClient} is constructed it calls {@link
 * NameEscaping#configure(NameConfig)}, which sets a global override that is consulted before the
 * environment variable. The env var is used only as a fallback when no YAML configuration was
 * loaded (i.e. when no {@link io.openlineage.client.OpenLineageClient} was constructed, or when the
 * {@code name.escaping} key is absent from the YAML file).
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class NameConfig implements MergeConfig<NameConfig> {

  /**
   * When {@code true}, automatic dot-escaping of name segments is enabled. Defaults to {@code
   * null}, which is treated as {@code false} (escaping disabled) by {@link NameEscaping}.
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
