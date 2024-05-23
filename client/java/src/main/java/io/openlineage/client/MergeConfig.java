/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client;

import java.util.HashMap;
import java.util.Map;

/**
 * Contains methods to merge different config classes. It is mainly used to merge config entries
 * from YAML files with config entries provided in another way (like integration specific
 * configuration mechanism: SparkConf, FlinkConf).
 *
 * <p>The general behaviour for classes implementing it is: implement a method that allows creating
 * new config class which merges each property. When merging a non-value is taken and if two values
 * are present then the argument of a method has higher precedence. If a property implements
 * MergeConfig interface, then interface method is called. Merging logic needs to be aware of
 * default values so that it does not overwrite a value with default entry.
 */
public interface MergeConfig<T> {

  /**
   * Merges current instance with instance provided. Checks if argument provided is non-null.
   *
   * @param t object to be merged with
   * @return merged value
   */
  default T mergeWith(T t) {
    if (t != null && (t instanceof MergeConfig)) {
      return mergeWithNonNull(t);
    }
    return t;
  }

  /**
   * Creates union of maps. In case of key collision, second parameter overwrites the first. Does
   * null checking to avoid NPE
   *
   * @param base base value
   * @param overwrite overwrite value
   * @return merged map
   */
  default Map mergePropertyWith(Map base, Map overwrite) {
    if (base == null) {
      return overwrite;
    } else if (overwrite != null) {
      Map result = new HashMap<>(base);
      ((Map<?, ?>) result).putAll(overwrite);
      return result;
    }
    return base;
  }

  /**
   * Checks for nulls to avoid NPE. If arguments are of different classes, the latter is returned.
   * If they implement MergeConfig interface, then interface methods are used to merge values.
   *
   * @param base base value
   * @param overwrite overwrite value
   * @return merged config entry
   * @param <T> generic type of merged object
   */
  default <T> T mergePropertyWith(T base, T overwrite) {
    if (base == null) {
      return overwrite;
    } else if (overwrite == null) {
      return base;
    } else if (!base.getClass().equals(overwrite.getClass())) {
      // if types are different -> always take the overwrite type
      return overwrite;
    } else if (base instanceof MergeConfig) {
      // call deep overwrite
      return ((MergeConfig<T>) base).mergeWith(overwrite);
    } else if (overwrite != null) {
      // simple replace
      return overwrite;
    } else {
      return base;
    }
  }

  /**
   * Overwrites base value with overwrite value only when it is does not equal default
   *
   * @param base base value
   * @param overwrite overwrite value
   * @param def default value
   * @return merged object
   * @param <T> generic type of merged object
   */
  default <T> T mergeWithDefaultValue(T base, T overwrite, T def) {
    if (overwrite == null && base == null) {
      return def;
    } else if (overwrite != null && !overwrite.equals(def)) {
      return overwrite;
    } else {
      return base;
    }
  }

  /**
   * Method to create new config class based on current instance and non-null argument value. In
   * most cases, implementation needs to create a new instance of T, while merging all the
   * properties.
   *
   * @param t overwrite value
   * @return merged config entry
   */
  T mergeWithNonNull(T t);
}
