package io.openlineage.spark.agent.lifecycle.plan;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class JdbcUrlSanitizerImpl implements JdbcUrlSanitizer {

  Map<String, JdbcUrlSanitizer> sanitizers = ImmutableMap.of(
      "sqlserver", new SqlServerSanitizer(),
      "postgresql", new PostgresThinSanitizer()
      "oracle", new OracleThinSanitizer()
  );

  private JdbcUrlSanitizer defaultSanitizer = new CommonSanitizer();

  @Override
  public String sanitize(String jdbcUrl) {
    String url = jdbcUrl.replaceFirst("jdbc:", "");
    try {
      String provider = jdbcUrl.substring(5, jdbcUrl.indexOf(':', 5)).toLowerCase();

      JdbcUrlSanitizer sanitizer = sanitizers.getOrDefault(provider, defaultSanitizer);
      return sanitizer.sanitize(url);
    } catch (Exception e) {
      return url;
    }
  }

  private static Map<String, String> split(String data, String outerDelimiter, String entryDelimiter){
    return Arrays.stream(data.split(outerDelimiter))
        .map(s -> s.split(entryDelimiter))
        .collect(Collectors.toMap(
            a -> a[0].toLowerCase(),
            a -> a[1]
        ));
  }

  private static String combine(String left, String db) {
    return String.format("%s/%s", left, db);
  }

  public static class CommonSanitizer implements JdbcUrlSanitizer {
    Pattern partsPattern = Pattern.compile("(.*//(.+)/\\w+)");

    @Override
    public String sanitize(String jdbcUrl) {
      Matcher partsMatcher = partsPattern.matcher(jdbcUrl);
      if (partsMatcher.find()) {
        String fullPart = partsMatcher.group(1);
        String hostPart = partsMatcher.group(2);
        String hostPartCleaned = hostPart.replaceAll("(.*?):(.*?)@", "$3");
        return fullPart.replace(hostPart, hostPartCleaned);
      }
      return jdbcUrl;
    }
  }

  private static class SqlServerSanitizer implements JdbcUrlSanitizer {
    private static final String DB_PROP_NAME = "databasename";
    private static final String SERVER_NAME_PROP_NAME = "servername";

    @Override
    public String sanitize(String jdbcUrl) {
      int keyValueIndex = jdbcUrl.indexOf(';');
      if (keyValueIndex != 0) {
        String leftPart = jdbcUrl.substring(0, keyValueIndex);
        String rightPart = jdbcUrl.substring(keyValueIndex + 1);
        Map<String, String> properties = split(rightPart, ";", "=");
        String dbName = properties.getOrDefault(DB_PROP_NAME, "");
        String serverName = properties.getOrDefault(SERVER_NAME_PROP_NAME, "");
        return combine(leftPart + serverName, dbName);
      }

      return jdbcUrl;
    }
  }

  private class PostgresThinSanitizer implements JdbcUrlSanitizer {

    @Override
    public String sanitize(String jdbcUrl) {
      int propIndex = jdbcUrl.indexOf('?');
      return propIndex == -1 ? jdbcUrl : jdbcUrl.substring(0, propIndex);
    }
  }

  private class OracleThinSanitizer implements JdbcUrlSanitizer {

    @Override
    public String sanitize(String jdbcUrl) {
      int protocol = jdbcUrl.indexOf(":", jdbcUrl.indexOf(":") + 1) + 1;
      int var8 = jdbcUrl.indexOf('/', protocol+1);
      int var9 = jdbcUrl.indexOf('@', protocol+1);

      return null;
    }
  }

  private class TeradataSanitizer implements JdbcUrlSanitizer {

    @Override
    public String sanitize(String jdbcUrl) {
      return null;
    }
  }

}