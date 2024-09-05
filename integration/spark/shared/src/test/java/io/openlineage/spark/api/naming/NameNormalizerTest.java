/*
/* Copyright 2018-2024 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/
package io.openlineage.spark.api.naming;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class NameNormalizerTest {
  @Test
  @SuppressWarnings("PMD.JUnitTestContainsTooManyAsserts")
  void testNormalize() {
    assertEquals("", NameNormalizer.normalize(""));
    assertEquals("word", NameNormalizer.normalize("word"));
    assertEquals("more_than_one_words", NameNormalizer.normalize("@ more than one     words !"));
    assertEquals(
        "with_some_capitalized_words", NameNormalizer.normalize("with some Capitalized WORDS"));
    assertEquals(
        "this_includes_some_parenthesis",
        NameNormalizer.normalize("this (includes some) parenthesis"));
    assertEquals(
        "this_on_the_other_hand_has_hyphens",
        NameNormalizer.normalize("this - on the other hand -- has hyphens"));
    assertEquals(
        "now_we_have_a_complex_name_with_hyphens_too",
        NameNormalizer.normalize("now we have (a complex) name - with hyphens too"));
    assertEquals(
        "in_glue_aws_we_can_have_slashes",
        NameNormalizer.normalize("in glue/aws we can have slashes"));
    assertEquals(
        "the_names_like_this_should_be_separated_are_they",
        NameNormalizer.normalize("theNamesLikeThisShouldBeSeparated - are they?"));
    assertEquals("test_xml_test", NameNormalizer.normalize("testXMLTest"));
    // After acronym if we have no space, the first word should be capitalized
    assertEquals("test_xm_ltest", NameNormalizer.normalize("testXMLtest"));
    assertEquals("test_xml_test", NameNormalizer.normalize("test XML test"));
    assertEquals(
        "foreign_letters_ase_are_supported",
        NameNormalizer.normalize("foreignLetters ąśé•¶€ are supported"));
    assertEquals("but_to_a_degree", NameNormalizer.normalize("but to a degree 这是一个例子"));
    assertEquals(
        "323_digits123_should45_be_also34_fine89",
        NameNormalizer.normalize("323digits123Should45BE_ALSO34 fine89"));

    assertEquals("a_test_application", NameNormalizer.normalize("A Test Application"));
    assertEquals("my_test_application", NameNormalizer.normalize("MyTestApplication"));
    assertEquals("my_xml_based_application", NameNormalizer.normalize("MyXMLBasedApplication"));
    assertEquals("jdbc_relation_application", NameNormalizer.normalize("JDBCRelationApplication"));
    assertEquals(
        "test_with_a_single_letter_between_words",
        NameNormalizer.normalize("Test With a Single LetterBetweenWords"));
  }
}
