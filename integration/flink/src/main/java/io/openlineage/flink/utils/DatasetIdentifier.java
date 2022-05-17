/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.flink.utils;

import lombok.Value;

@Value
public class DatasetIdentifier {
  String name;
  String namespace;
}
