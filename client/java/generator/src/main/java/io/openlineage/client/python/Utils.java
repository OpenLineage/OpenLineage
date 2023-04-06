package io.openlineage.client.python;

public class Utils {
  public static String nestString(String content, int nestLevel) {
    return new String(new char[nestLevel * 4]).replace('\0', ' ') + content;
  }
}
