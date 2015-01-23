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
package org.apache.aurora.scheduler.app;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.application.modules.LocalServiceRegistry;

import static java.util.Objects.requireNonNull;

/**
 * Wraps the provided LocalServiceRegistry and optionally overrides the hostname it provides.
 */
public class LocalServiceRegistryWithOverrides {
  public static class Settings {
    private final Optional<String> zkLocalDnsNameOverride;

    public Settings(Optional<String> zkLocalDnsNameOverride) {
      if (zkLocalDnsNameOverride.isPresent()) {
        /* Force resolution of the DNS address passed in to ensure it's valid */
        try {
          InetAddress.getByName(zkLocalDnsNameOverride.get());
        } catch (UnknownHostException e) {
          throw new IllegalStateException(
              "Failed to resolve hostname supplied by -hostname", e);
        }
      }
      this.zkLocalDnsNameOverride = zkLocalDnsNameOverride;
    }

    public Optional<String> getZkLocalDnsNameOverride() {
      return zkLocalDnsNameOverride;
    }
  }

  private final LocalServiceRegistry wrapped;
  private final Optional<String> zkLocalDnsNameOverride;

  @Inject
  public LocalServiceRegistryWithOverrides(
      LocalServiceRegistry registry,
      Settings settings) {
    this.wrapped = requireNonNull(registry);
    this.zkLocalDnsNameOverride = settings.getZkLocalDnsNameOverride();
  }

  private Map<String, InetSocketAddress> applyDnsOverrides(
      Map<String, InetSocketAddress> services) {
    final InetAddress inetAddress;
    try {
      inetAddress = InetAddress.getByName(this.zkLocalDnsNameOverride.get());
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to resolve address.", e);
    }
    return ImmutableMap.copyOf(
        Maps.transformValues(services, new Function<InetSocketAddress, InetSocketAddress>() {
          @Override
          public InetSocketAddress apply(InetSocketAddress input) {
            return new InetSocketAddress(inetAddress, input.getPort());
          }
        }));
  }

  public Map<String, InetSocketAddress> getAuxiliarySockets() {
    Map<String, InetSocketAddress> auxSockets = wrapped.getAuxiliarySockets();
    if (zkLocalDnsNameOverride.isPresent()) {
      return applyDnsOverrides(auxSockets);
    } else {
      return auxSockets;
    }
  }
}
