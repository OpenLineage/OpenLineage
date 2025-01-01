/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.transports;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.openlineage.client.MergeConfig;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Slf4j
@AllArgsConstructor
@ToString
public class FacetsConfig implements MergeConfig<FacetsConfig> {

  /* This property is deprecated. Disable facets with "&lt;facet name&gt;.disabled=true" instead */
  @Getter(onMethod_ = {@Deprecated})
  @JsonProperty("disabled")
  private String[] deprecatedDisabledFacets;

  @Getter
  @JsonProperty("custom_environment_variables")
  private String[] customEnvironmentVariables;

  /* The field is deserialized with {@link #onOtherProperty}. It is lazily initialized */
  @Getter @Setter private Map<String, Boolean> disabledFacets;

  @SuppressWarnings({"PMD.UseVarargs", "PMD.ArrayIsStoredDirectly"})
  public void setDeprecatedDisabledFacets(String[] disabledFacets) {
    if (disabledFacets.length > 0) {
      log.warn(
          "Deprecation warning: The property 'disabledFacets' is deprecated and will be removed in the future. Use 'facets.<name of disabled facet>.disabled=true' instead");
    }
    this.deprecatedDisabledFacets = disabledFacets;
  }

  /**
   * This method accepts every other property we may receive in configuration. The only supported
   * properties now are the "disabled" facet properties. The rest is silently ignored.
   *
   * @param key the name of the property which is not directly deserialized to any other field
   * @param value the value of the property. Can be a map for nested fields
   */
  @JsonAnySetter
  public void onOtherProperty(String key, Object value) {
    asDisabledFacetProperties(key, value)
        .forEach(
            facetProperty -> {
              if (disabledFacets == null) {
                disabledFacets = new HashMap<>();
              }
              disabledFacets.put(facetProperty.getFacetName(), facetProperty.isDisabled());
            });
  }

  /**
   * Tries parse the property as facet "disable" property setting. The names of the facets can be
   * separated.by.dots and one prefix can include many disabled facets. For this reason this
   * function can return a list of disabled facet properties.
   */
  public static List<DisabledFacetProperty> asDisabledFacetProperties(String key, Object value) {
    /*
    The algorithm:
    - Flatten {prefix: {facet: {field: value, disabled: value}}, to to {prefix.facet.field: value, prefix.facet.disabled: value}
    - Filter the records without ".disabled" suffix
    - Try to treat each value as boolean
    - Filter the values that are not valid booleans
    - Truncate ".disabled" suffix
     */
    if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<String, Object> inputValueMap = (Map<String, Object>) value;
      Map<String, Object> flattenedMap = new HashMap<>();
      flattenMapHelper(inputValueMap, key, flattenedMap);
      return flattenedMap.entrySet().stream()
          .filter(entry -> entry.getKey().endsWith(".disabled"))
          .map(
              entry ->
                  new AbstractMap.SimpleEntry<>(
                      entry.getKey(), toBoolean(entry.getKey(), entry.getValue())))
          .filter(e -> e.getValue().isPresent())
          .map(
              entry ->
                  new DisabledFacetProperty(
                      FacetsConfig.truncateDisabledSuffix(entry.getKey()), entry.getValue().get()))
          .collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  private static String truncateDisabledSuffix(String key) {
    return key.substring(0, key.length() - ".disabled".length());
  }

  private static Optional<Boolean> toBoolean(String key, Object value) {
    if (value instanceof Boolean) {
      return Optional.of((Boolean) value);
    } else if (value instanceof String) {
      if ("true".equals(value)) {
        return Optional.of(true);
      } else if ("false".equals(value)) {
        return Optional.of(false);
      }
    }
    log.warn("The property {} is not a valid boolean", key);
    return Optional.empty();
  }

  /**
   * Recursively visit map and once it reaches the end of the nested chain of maps, it connects the
   * keys with a dot and puts one entry to the flattened map.
   *
   * @param map the map for traversal
   * @param prefix the prefix for the current nest level
   * @param flattenedMap the map where the output will be put
   */
  private static void flattenMapHelper(
      Map<String, Object> map, String prefix, Map<String, Object> flattenedMap) {
    for (Map.Entry<String, Object> entry : map.entrySet()) {
      String key = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
      Object value = entry.getValue();
      if (value instanceof Map) {
        @SuppressWarnings("unchecked")
        Map<String, Object> mapValue = (Map<String, Object>) value;
        flattenMapHelper(mapValue, key, flattenedMap);
      } else {
        flattenedMap.put(key, value);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public FacetsConfig mergeWithNonNull(FacetsConfig facetsConfig) {
    return new FacetsConfig(
        mergePropertyWith(deprecatedDisabledFacets, facetsConfig.deprecatedDisabledFacets),
        mergePropertyWith(customEnvironmentVariables, facetsConfig.customEnvironmentVariables),
        mergePropertyWith(disabledFacets, facetsConfig.disabledFacets));
  }

  @Data
  public static class DisabledFacetProperty {
    private final String facetName;
    private final boolean disabled;
  }

  /**
   * Merges deprecated and new mechanism for disabled facets into one effective list
   *
   * @return an array of disabled facets including facets from new and deprecated mechanisms.
   */
  public String[] getEffectiveDisabledFacets() {
    Set<String> disabledFacetsSet = new HashSet<>();
    if (getDeprecatedDisabledFacets() != null) {
      for (String deprecatedDisabledFacet : getDeprecatedDisabledFacets()) {
        disabledFacetsSet.add(deprecatedDisabledFacet);
      }
    }
    if (getDisabledFacets() != null) {
      getDisabledFacets()
          .forEach(
              (facetName, disabled) -> {
                if (disabled) {
                  disabledFacetsSet.add(facetName);
                } else {
                  disabledFacetsSet.remove(facetName);
                }
              });
    }
    return disabledFacetsSet.toArray(new String[0]);
  }
}
