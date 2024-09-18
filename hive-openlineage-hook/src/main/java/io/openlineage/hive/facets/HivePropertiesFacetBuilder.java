/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.facets;

import io.openlineage.hive.api.OpenLineageContext;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;

@Slf4j
public class HivePropertiesFacetBuilder {
  private static final Set<String> DEFAULT_ALLOWED_PROPERTIES =
      new HashSet<>(Arrays.asList("hive.execution.engine", "hive.query.id"));
  private static final String ALLOWED_PROPERTIES_KEY = "hive.openlineage.capturedProperties";
  private final Configuration conf;
  private final Set<String> allowedProperties;

  public HivePropertiesFacetBuilder(OpenLineageContext olContext) {
    conf = olContext.getHadoopConf();
    allowedProperties =
        conf.get(ALLOWED_PROPERTIES_KEY) != null
            ? Arrays.stream(conf.get(ALLOWED_PROPERTIES_KEY).split(",")).collect(Collectors.toSet())
            : DEFAULT_ALLOWED_PROPERTIES;
  }

  public Map<String, Object> getFilteredConf() {
    Map<String, Object> result = new HashMap<>();
    conf.iterator()
        .forEachRemaining(
            entry -> {
              String key = entry.getKey();
              if (allowedProperties.contains(key)) {
                result.putIfAbsent(key, entry.getValue());
              }
            });
    return result;
  }

  public HivePropertiesFacet build() {
    return new HivePropertiesFacet(getFilteredConf());
  }
}
