package io.openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Generic implementation of {@link JdbcUrlSanitizer} Removes username and password from jdbc url
 */
public class JdbcUrlSanitizerImpl implements JdbcUrlSanitizer {

  public static final String SLASH_DELIMITER_USER_PASSWORD_REGEX =
      "[A-Za-z0-9_%]+//?[A-Za-z0-9_%]*@";
  public static final String COLON_DELIMITER_USER_PASSWORD_REGEX =
      "([/|,])[A-Za-z0-9_%]+:?[A-Za-z0-9_%]*@";
  public static final Pattern EXTRACT_KEY_REGEX = Pattern.compile("([\\w|%]*)$");

  public static final String KEY_VALUE_FORMAT = "(?i)%s%s%s%s?";
  public static final String[][] KEY_VALUE_STYLE =
      new String[][] {
        {";", "="}, // sqlserver, db2, cassandra, phoenix, hive2
        {"&", "="}, // postgres, snowflake, redshift,mongo
        {",", "="}, // mysql, teradata
        {"\\)", "="}, // mysql
        {":", "="}, // db2 (user, password)
      };
  private static final Set<String> FILTERED_KEYS = ImmutableSet.of("user", "username", "password");

  @Override
  public String sanitize(String jdbcUrl) {
    jdbcUrl = jdbcUrl.substring(5);
    jdbcUrl = jdbcUrl.replaceAll(SLASH_DELIMITER_USER_PASSWORD_REGEX, "@");
    final String urlWithoutUserSpec = jdbcUrl.replaceAll(COLON_DELIMITER_USER_PASSWORD_REGEX, "$1");

    List<String> pairToRemove = new ArrayList<>();
    for (String[] pair : KEY_VALUE_STYLE) {
      Map<String, List<String>> props = split(urlWithoutUserSpec, pair[0], pair[1]);
      for (String key : FILTERED_KEYS) {
        for (String value : props.getOrDefault(key, Collections.emptyList())) {
          pairToRemove.add(String.format(KEY_VALUE_FORMAT, key, pair[1], value, pair[0]));
        }
      }
    }
    return pairToRemove.stream()
        .reduce(urlWithoutUserSpec, (prev, cur) -> prev.replaceAll(cur, ""));
  }

  private static Map<String, List<String>> split(
      String data, String outerDelimiter, String entryDelimiter) {
    Map<String, List<String>> result = new HashMap<>();
    for (String pair : data.split(outerDelimiter)) {
      String[] parts = pair.split(entryDelimiter);
      if (parts.length == 2) {
        Matcher m = EXTRACT_KEY_REGEX.matcher(parts[0]);
        m.find();
        String key = m.group(1).toLowerCase();
        result.putIfAbsent(key, new ArrayList<>());
        result.get(key).add(parts[1]);
      }
    }
    return result;
  }
}
