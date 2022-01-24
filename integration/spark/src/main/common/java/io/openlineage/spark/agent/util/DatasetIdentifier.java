/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.util;

import lombok.Value;

@Value
public class DatasetIdentifier {
  String name;
  String namespace;
}
