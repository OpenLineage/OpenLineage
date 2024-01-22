/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EventMatcher {

  public static boolean match(Object expected, Object result) {
    if (expected instanceof Map) {
      Map<?, ?> expectedMap = (Map<?, ?>) expected;
      Map<?, ?> resultMap = (Map<?, ?>) result;

      for (Object key : expectedMap.keySet()) {
        if (!resultMap.containsKey(key)) {
          log.error("Key " + key + " not in event " + resultMap + "\nExpected " + expectedMap);
          return false;
        }
        if (!match(expectedMap.get(key), resultMap.get(key))) {
          log.error(
              "For key "
                  + key
                  + ", value "
                  + expectedMap.get(key)
                  + " not equals "
                  + resultMap.get(key)
                  + "\nExpected "
                  + expectedMap
                  + ", request "
                  + resultMap);
          return false;
        }
      }
    } else if (expected instanceof List) {
      List<?> expectedList = (List<?>) expected;
      List<?> resultList = (List<?>) result;

      if (expectedList.size() != resultList.size()) {
        log.error(
            "Length does not match: expected "
                + expectedList.size()
                + " result: "
                + resultList.size());
        return false;
      }

      for (int i = 0; i < expectedList.size(); i++) {
        if (!match(expectedList.get(i), resultList.get(i))) {
          log.error(
              "List not matched expected: "
                  + expectedList.get(i)
                  + " result: "
                  + resultList.get(i));
          return false;
        }
      }
    } else if (expected instanceof String) {
      if (!expected.equals(result)) {
        log.error("Expected value " + expected + " does not equal result " + result);
        return false;
      }
    } else if (!expected.equals(result)) {
      log.error(
          "Object of type "
              + expected.getClass().getSimpleName()
              + ": "
              + expected
              + " does not match "
              + result);
      return false;
    }
    return true;
  }

  public static boolean checkMatches(
      List<Map<String, Object>> expectedEvents, List<Map<String, Object>> actualEvents) {
    for (Map<String, Object> expected : expectedEvents) {
      Map<String, Object> expectedBody = (Map<String, Object>) expected.get("body");
      String expectedJobName = (String) ((Map<String, Object>) expectedBody.get("job")).get("name");
      boolean isCompared = false;

      for (Map<String, Object> actual : actualEvents) {
        Map<String, Object> actualBody = (Map<String, Object>) actual.get("body");

        if (expectedBody.get("eventType").equals(actualBody.get("eventType"))
            && expectedJobName.equals(((Map<String, Object>) actualBody.get("job")).get("name"))) {
          isCompared = true;
          if (!match(expectedBody, actualBody)) {
            log.info("Failed to compare expected " + expectedBody + "\nwith actual " + actualBody);
            return false;
          }
          break;
        }
      }

      if (!isCompared) {
        log.info(
            "Not found event comparable to "
                + expectedBody.get("eventType")
                + " - "
                + expectedJobName);
        return false;
      }
    }
    return true;
  }
}
