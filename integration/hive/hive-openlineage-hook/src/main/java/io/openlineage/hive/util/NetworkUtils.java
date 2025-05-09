/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
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
