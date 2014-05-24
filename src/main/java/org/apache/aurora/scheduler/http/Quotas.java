/**
 *
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

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 * Servlet that exposes allocated resource quotas.
 */
@Path("/quotas")
public class Quotas {

  private final Storage storage;

  @Inject
  Quotas(Storage storage) {
    this.storage = Preconditions.checkNotNull(storage);
  }

  /**
   * Dumps allocated resource quotas.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOffers(@QueryParam("role") final String role) {
    return storage.weaklyConsistentRead(new Work.Quiet<Response>() {
      @Override
      public Response apply(StoreProvider storeProvider) {
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
      }
    });
  }

  private static final Function<IResourceAggregate, ResourceAggregateBean> TO_BEAN =
      new Function<IResourceAggregate, ResourceAggregateBean>() {
        @Override
        public ResourceAggregateBean apply(IResourceAggregate quota) {
          return new ResourceAggregateBean(quota.getNumCpus(), quota.getRamMb(), quota.getDiskMb());
        }
      };

  private static final class ResourceAggregateBean {
    private final double cpu;
    private final long ramMb;
    private final long diskMb;

    private ResourceAggregateBean(double cpu, long ramMb, long diskMb) {
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
