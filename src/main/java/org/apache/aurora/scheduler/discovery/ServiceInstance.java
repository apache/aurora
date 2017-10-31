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
package org.apache.aurora.scheduler.discovery;

import java.util.Map;
import java.util.Objects;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;

import static java.util.Objects.requireNonNull;

public class ServiceInstance {
  private final Endpoint serviceEndpoint;
  private final Map<String, Endpoint> additionalEndpoints;

  // Legacy field.
  private final String status;

  /**
   * Default constructor for gson, needed to inject a default empty map.
   */
  ServiceInstance() {
    this.serviceEndpoint = null;
    this.additionalEndpoints = ImmutableMap.of();
    this.status = "ALIVE";
  }

  public ServiceInstance(Endpoint serviceEndpoint, Map<String, Endpoint> additionalEndpoints) {
    this.serviceEndpoint = requireNonNull(serviceEndpoint);
    this.additionalEndpoints = requireNonNull(additionalEndpoints);
    this.status = "ALIVE";
  }

  public Endpoint getServiceEndpoint() {
    return serviceEndpoint;
  }

  public Map<String, Endpoint> getAdditionalEndpoints() {
    return additionalEndpoints;
  }

  public String getStatus() {
    return status;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ServiceInstance)) {
      return false;
    }

    ServiceInstance other = (ServiceInstance) obj;
    return Objects.equals(serviceEndpoint, other.serviceEndpoint)
        && Objects.equals(additionalEndpoints, other.additionalEndpoints)
        && status.equals(other.status);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceEndpoint, additionalEndpoints, status);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("serviceEndpoint", serviceEndpoint)
        .add("additionalEndpoints", additionalEndpoints)
        .add("status", status)
        .toString();
  }

  public static class Endpoint {
    private final String host;
    private final int port;

    public Endpoint(String host, int port) {
      this.host = requireNonNull(host);
      this.port = port;
    }

    public String getHost() {
      return host;
    }

    public int getPort() {
      return port;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof Endpoint)) {
        return false;
      }

      Endpoint other = (Endpoint) obj;
      return Objects.equals(host, other.host)
          && port == other.port;
    }

    @Override
    public int hashCode() {
      return Objects.hash(host, port);
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
          .add("host", host)
          .add("port", port)
          .toString();
    }
  }
}
