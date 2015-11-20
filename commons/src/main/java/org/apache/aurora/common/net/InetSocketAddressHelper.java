/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.common.net;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Set;

/**
 * A utility that can parse [host]:[port] pairs or :[port] designators into instances of
 * {@link java.net.InetSocketAddress}. The literal '*' can be specified for port as an alternative
 * to '0' to indicate any local port.
 *
 * @author John Sirois
 */
public final class InetSocketAddressHelper {

  /**
   * Attempts to parse an endpoint spec into an InetSocketAddress.
   *
   * @param value the endpoint spec
   * @return a parsed InetSocketAddress
   * @throws NullPointerException     if {@code value} is {@code null}
   * @throws IllegalArgumentException if {@code value} cannot be parsed
   */
  public static InetSocketAddress parse(String value) {
    Preconditions.checkNotNull(value);

    String[] spec = value.split(":", 2);
    if (spec.length != 2) {
      throw new IllegalArgumentException("Invalid socket address spec: " + value);
    }

    String host = spec[0];
    int port = asPort(spec[1]);

    return StringUtils.isEmpty(host)
        ? new InetSocketAddress(port)
        : InetSocketAddress.createUnresolved(host, port);
  }

  /**
   * Attempts to return a usable String given an InetSocketAddress.
   *
   * @param value the InetSocketAddress.
   * @return the String representation of the InetSocketAddress.
   */
  public static String toString(InetSocketAddress value) {
    Preconditions.checkNotNull(value);
    return value.getHostName() + ":" + value.getPort();
  }

  private static int asPort(String port) {
    if ("*".equals(port)) {
      return 0;
    }
    try {
      return Integer.parseInt(port);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid port: " + port, e);
    }
  }

  private InetSocketAddressHelper() {
    // utility
  }
}
