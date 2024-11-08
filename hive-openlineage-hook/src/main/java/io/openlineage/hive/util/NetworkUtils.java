/*
 * Copyright 2024 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.openlineage.hive.util;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Collections;
import java.util.Enumeration;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NetworkUtils {

  public static final InetAddress LOCAL_IP_ADDRESS = findLocalInetAddress();

  public static InetAddress findLocalInetAddress() {
    try {
      InetAddress address = InetAddress.getLocalHost();
      if (address.isLoopbackAddress()) {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        for (NetworkInterface ni : Collections.list(networkInterfaces)) {
          Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
          for (InetAddress addr : Collections.list(inetAddresses)) {
            if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()) {
              if (addr instanceof Inet4Address) {
                return InetAddress.getByAddress(addr.getAddress());
              }
            }
          }
        }
        log.warn(
            "Your hostname, "
                + InetAddress.getLocalHost().getHostName()
                + ", resolves to a loopback address: "
                + address.getHostAddress()
                + ", but we couldn't find any external IP address!");
      }
      return address;
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }
}
