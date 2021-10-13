package io.openlineage.spark.agent.lifecycle.plan;

import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
      FILTERED_KEYS.stream()
          .filter(props::containsKey)
          .forEach(
              k ->
                  props
                      .get(k)
                      .forEach(
                          v ->
                              pairToRemove.add(
                                  String.format(KEY_VALUE_FORMAT, k, pair[1], v, pair[0]))));
    }
    return pairToRemove.stream()
        .reduce(urlWithoutUserSpec, (prev, cur) -> prev.replaceAll(cur, ""));
  }

  private static Map<String, List<String>> split(
      String data, String outerDelimiter, String entryDelimiter) {
    return Arrays.stream(data.split(outerDelimiter))
        .map(s -> s.split(entryDelimiter))
        .filter(s -> s.length == 2)
        .collect(
            Collectors.toMap(
                a -> {
                  Matcher m = EXTRACT_KEY_REGEX.matcher(a[0]);
                  m.find();
                  return m.group(1);
                },
                a -> {
                  List<String> l = new ArrayList<>();
                  l.add(a[1].trim());
                  return l;
                },
                (a, b) -> {
                  a.addAll(b);
                  return a;
                }));
  }
}
