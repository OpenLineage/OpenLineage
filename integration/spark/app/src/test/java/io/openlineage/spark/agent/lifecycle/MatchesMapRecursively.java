/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.Versions;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Condition;

/**
 * Custom Condition writen for recursive comparison of Map and List with ability to ignore specified
 * Map keys AssertJ Built-in recursive comparison is not working with Map type, it can ignore only
 * object properties Example usage: assertThat(actualMap).satisfies(new
 * MatchesMapRecursively(expectedMap, new HashSet<>(Arrays.asList("runId"))));
 *
 * @see AbstractObjectAssert#usingRecursiveComparison()
 */
@Slf4j
public class MatchesMapRecursively extends Condition<Map<String, Object>> {

  public MatchesMapRecursively(Map<String, Object> target, Set<String> ommittedKeys) {
    super(
        MatchesMapRecursively.predicate(target, ommittedKeys),
        "matches snapshot fields %s",
        target);
  }

  public MatchesMapRecursively(Map<String, Object> target) {
    super(
        MatchesMapRecursively.predicate(target, new HashSet<>()),
        "matches snapshot fields %s",
        target);
  }

  public static Predicate<List<Object>> predicate(List<Object> target, Set<String> omittedKeys) {
    return (list) -> {
      if (target.size() != list.size()) {
        log.error(
            "Passed list size {} does not match target list size {}", list.size(), target.size());
        return false;
      }
      for (int i = 0; i < target.size(); i++) {
        boolean eq;
        if (target.get(i) instanceof Map) {
          eq =
              MatchesMapRecursively.predicate((Map<String, Object>) target.get(i), omittedKeys)
                  .test((Map<String, Object>) target.get(i));
        } else if (target.get(i) instanceof List) {
          eq =
              MatchesMapRecursively.predicate((List<Object>) target.get(i), omittedKeys)
                  .test((List<Object>) list.get(i));
        } else if (list.get(i) == null) {
          eq = true;
        } else {
          eq = target.get(i).equals(list.get(i));
        }
        if (!eq) {
          log.error("Passed object {} does not match target object {}", list.get(i), target.get(i));
          return false;
        }
      }
      return true;
    };
  }

  public static Predicate<Map<String, Object>> predicate(
      Map<String, Object> target, Set<String> omittedKeys) {
    return (map) -> {
      if (!map.keySet().containsAll(target.keySet())) {
        log.error("Object keys {} does not match target keys {}", map.keySet(), target.keySet());
        return false;
      }
      for (String k : target.keySet()) {
        if (omittedKeys.contains(k)) {
          continue;
        }
        Object val = map.get(k);
        boolean eq;
        if (val instanceof Map) {
          eq =
              MatchesMapRecursively.predicate((Map<String, Object>) target.get(k), omittedKeys)
                  .test((Map<String, Object>) val);
        } else if (val instanceof List) {
          eq =
              MatchesMapRecursively.predicate((List<Object>) target.get(k), omittedKeys)
                  .test((List<Object>) val);
        } else if (k.equals("_producer") || k.equals("producer")) {
          eq = Versions.OPEN_LINEAGE_PRODUCER_URI.toString().equals(val);
        } else if (val == null) {
          eq = true;
        } else {
          eq = val.equals(target.get(k));
        }
        if (!eq) {
          log.error("Passed object {} does not match target object {}", map.get(k), target.get(k));
          return false;
        }
      }
      return true;
    };
  }
}
