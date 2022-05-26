/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.lifecycle;

import java.util.Locale;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class ExecutionContextTest {

  @ParameterizedTest
  @CsvSource({
    "A Test Application,a_test_application",
    "MyTestApplication,my_test_application",
    "MyXMLBasedApplication,my_xml_based_application",
    "JDBCRelationApplication,jdbc_relation_application",
    "Test With a Single LetterBetweenWords,test_with_a_single_letter_between_words"
  })
  public void testCamelCaseToSnakeCase(String appName, String expected) {
    String actual =
        appName.replaceAll(ExecutionContext.CAMEL_TO_SNAKE_CASE, "_$1").toLowerCase(Locale.ROOT);
    Assertions.assertEquals(expected, actual);
  }
}
