/*
 * SPDX-License-Identifier: Apache-2.0
 */

package io.openlineage.proxy.api.models;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

/** ConsoleLineageStream pushes events to stdout */
@Slf4j
public class ConsoleLineageStream extends LineageStream {
  public ConsoleLineageStream() {
    super(Type.CONSOLE);
  }

  @Override
  public void collect(@NonNull String eventAsString) {
    log.info(eventAsString);
  }
}
