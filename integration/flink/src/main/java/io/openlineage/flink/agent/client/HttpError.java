/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.flink.agent.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
@RequiredArgsConstructor
public class HttpError {
  protected Integer code;
  @NonNull protected String message;
  protected String details;
}
