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
package org.apache.aurora.scheduler.http;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IResource;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.codehaus.jackson.annotate.JsonProperty;

import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.DISK_MB;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;

/**
 * Servlet that exposes allocated resource quotas.
 */
@Path("/quotas")
public class Quotas {

  private final Storage storage;

  @Inject
  Quotas(Storage storage) {
    this.storage = Objects.requireNonNull(storage);
  }

  /**
   * Dumps allocated resource quotas.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getQuotas(@QueryParam("role") final String role) {
    return storage.read(storeProvider -> {
      Map<String, IResourceAggregate> quotas;
      if (role == null) {
        quotas = storeProvider.getQuotaStore().fetchQuotas();
      } else {
        Optional<IResourceAggregate> quota = storeProvider.getQuotaStore().fetchQuota(role);
        if (quota.isPresent()) {
          quotas = ImmutableMap.of(role, quota.get());
        } else {
          quotas = ImmutableMap.of();
        }
      }

      return Response.ok(Maps.transformValues(quotas, TO_BEAN)).build();
    });
  }

  private static final Function<IResourceAggregate, ResourceAggregateBean> TO_BEAN =
      quota -> new ResourceAggregateBean(
          getResource(quota.getResources(), CPUS).getNumCpus(),
          getResource(quota.getResources(), RAM_MB).getRamMb(),
          getResource(quota.getResources(), DISK_MB).getDiskMb());

  private static IResource getResource(Set<IResource> resources, ResourceType type) {
    return resources.stream()
        .filter(e -> ResourceType.fromResource(e).equals(type))
        .findFirst()
        .orElseThrow(() ->
            new IllegalArgumentException("Missing resource definition for " + type));
  }

  private static final class ResourceAggregateBean {
    private final double cpu;
    private final long ramMb;
    private final long diskMb;

    ResourceAggregateBean(double cpu, long ramMb, long diskMb) {
      this.cpu = cpu;
      this.ramMb = ramMb;
      this.diskMb = diskMb;
    }

    @JsonProperty("cpu_cores")
    public double getCpu() {
      return cpu;
    }

    @JsonProperty("ram_mb")
    public long getRamMb() {
      return ramMb;
    }

    @JsonProperty("disk_mb")
    public long getDiskMb() {
      return diskMb;
    }
  }
}
