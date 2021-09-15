package io.openlineage.spark.agent.lifecycle;

import io.openlineage.spark.agent.client.OpenLineageClient;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Condition;

/**
 * Custom Condition writen for recursive comparison of Map with ability to ignore specified Map keys
 * AssertJ Built-in recursive comparison is not working with Map type, it can ignore only object
 * properties Example usage: assertThat(actualMap).satisfies(new RecursiveMatcher(expectedMap, new
 * HashSet<>(Arrays.asList("runId"))));
 *
 * @see AbstractObjectAssert#usingRecursiveComparison()
 */
public class RecursiveMatcher extends Condition<Map<String, Object>> {

  public RecursiveMatcher(Map<String, Object> target, Set<String> ommittedKeys) {
    super(RecursiveMatcher.predicate(target, ommittedKeys), "matches snapshot fields %s", target);
  }

  public static Predicate<Map<String, Object>> predicate(
      Map<String, Object> target, Set<String> ommittedKeys) {
    return (map) -> {
      if (!map.keySet().containsAll(target.keySet())) {
        return false;
      }
      for (String k : target.keySet()) {
        if (!ommittedKeys.contains(k)) {
          continue;
        }
        Object val = map.get(k);
        boolean eq;
        if (val instanceof Map) {
          eq =
              RecursiveMatcher.predicate((Map<String, Object>) target.get(k), ommittedKeys)
                  .test((Map<String, Object>) val);
        } else if (k.equals("_producer") || k.equals("producer")) {
          eq = OpenLineageClient.OPEN_LINEAGE_CLIENT_URI.toString().equals(val);
        } else {
          eq = val.equals(target.get(k));
        }
        if (!eq) {
          return false;
        }
      }
      return true;
    };
  }
}
