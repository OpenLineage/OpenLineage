/*
/* Copyright 2018-2026 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.client.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.lang3.StringUtils;

/**
 * Custom deserializer for tags field in the format of key:value tags being separated by semicolon:
 * ";". Also handles "labels" which are single-value tags, and are not key:value pairs. Also handles
 * "source" which is the third element in the tag, and is optional.
 */
@SuppressWarnings("PMD.AvoidLiteralsInIfCondition")
public class TagsDeserializer extends JsonDeserializer<List<TagField>> {
  private String preprocessEscapes(String input) {
    if (input == null || input.isEmpty()) {
      return input;
    }

    StringBuilder result = new StringBuilder();
    boolean escaped = false;

    for (int i = 0; i < input.length(); i++) {
      char c = input.charAt(i);
      if (c == '\\' && !escaped) {
        escaped = true;
        continue;
      }

      if (escaped) {
        if (c == ':' || c == ';') {
          // For escaped special chars, use placeholder characters
          result.append((char) (c == ':' ? 1 : 2));
        } else {
          result.append('\\').append(c);
        }
        escaped = false;
      } else {
        result.append(c);
      }
    }

    if (escaped) {
      result.append('\\');
    }

    return result.toString();
  }

  private String restoreEscapes(String input) {
    return input.replace((char) 1, ':').replace((char) 2, ';');
  }

  @Override
  public List<TagField> deserialize(JsonParser jsonParser, DeserializationContext ctx)
      throws IOException {
    JsonNode node = jsonParser.getCodec().readTree(jsonParser);
    Stream<String> tagText;
    if (node.isArray()) {
      tagText =
          StreamSupport.stream(node.spliterator(), false)
              .map(JsonNode::asText)
              .filter(StringUtils::isNotBlank)
              .map(this::preprocessEscapes);
    } else if (node.isTextual()) {
      tagText =
          Arrays.stream(preprocessEscapes(node.asText()).split(";"))
              .filter(StringUtils::isNotBlank);
    } else {
      return Collections.emptyList();
    }

    return tagText
        .filter(StringUtils::isNotBlank)
        .flatMap(
            x -> {
              String[] elements = x.split(":");
              String key;
              String value = "true";
              String source = "CONFIG";
              if (elements.length == 0) {
                return Stream.empty();
              }

              // If we get malformed data, we skip the tag.
              if (StringUtils.isBlank(elements[0])) {
                return Stream.empty();
              }
              key = restoreEscapes(elements[0]).trim();
              if (elements.length >= 2) {
                // If we get malformed data, we skip the tag.
                if (StringUtils.isBlank(elements[1])) {
                  return Stream.empty();
                }
                value = restoreEscapes(elements[1]).trim();
              }
              if (elements.length >= 3) {
                // If elements.length > 3, truncate the rest of the elements - use only
                // first three ones.
                // If we get malformed data, we skip the tag.
                if (StringUtils.isBlank(elements[2])) {
                  return Stream.empty();
                }
                source = restoreEscapes(elements[2]).trim();
              }
              return Stream.of(new TagField(key, value, source));
            })
        .collect(Collectors.toList());
  }
}
