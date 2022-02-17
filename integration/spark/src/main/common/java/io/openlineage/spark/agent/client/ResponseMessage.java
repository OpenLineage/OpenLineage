/* SPDX-License-Identifier: Apache-2.0 */

package io.openlineage.spark.agent.client;

import lombok.Getter;
import lombok.ToString;
import lombok.Value;

@Value
@ToString
public class ResponseMessage<T> {
  @Getter protected int responseCode;
  @Getter protected T body;
  @Getter protected HttpError error;

  public boolean completedSuccessfully() {
    if (responseCode >= 400 && responseCode < 600 || responseCode == 0) { // non-2xx
      return false;
    }
    return true;
  }
}
