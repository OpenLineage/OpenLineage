/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api.naming;

import java.text.Normalizer;
import java.util.Locale;
import lombok.experimental.UtilityClass;

@UtilityClass
public class NameNormalizer {
  /** Normalizes the input stream into the version_with_underscores_only. */
  public static String normalize(String input) {
    /*
    First, trim non-letters on both ends.
     */
    String trimmed = input.replaceAll("^[^\\w]+", "").replaceAll("[^\\w]+$", "");
    /*
    At this point we break every letter with an accent (āăąēîïĩíĝġńñšŝśûůŷ) into two characters
    (a letter followed by its accent). Then we remove the accents.
     */
    String normalizedAccents =
        Normalizer.normalize(trimmed, Normalizer.Form.NFKD).replaceAll("\\p{M}", "");
    /*
    Now we can detect separate words.
    First we add separation for cases like the following: "camelCaseExample" -> "camel Case Example"
    Then we handle acronyms: "someACRONYMInside" -> "some ACRONYM Inside"
    We add some extra check that we do not make the second part at the beginning of the input. Otherwise, we would add
    unnecessary space at the beginning in some cases.
     */
    String withSeparatedWords =
        normalizedAccents
            .replaceAll("([a-z])([A-Z])", "$1 $2")
            .replaceAll("(?<!^)([A-Z])([a-z])", " $1$2")
            .replaceAll("(\\d)(?=\\D)", "$1 ");
    /*
    Now we replace non-letters with underscores and make everything lowercase.
     */
    return withSeparatedWords.toLowerCase(Locale.ROOT).replaceAll("[^a-zA-Z0-9]+", "_");
  }
}
