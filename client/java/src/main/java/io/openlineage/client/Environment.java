package io.openlineage.client;

// This class exists because it's not possible to mock System
public class Environment {
  public static String getEnvironmentVariable(String key) {
    return System.getenv(key);
  }
}
