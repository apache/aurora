/*
 * Copyright 2013 Twitter, Inc.
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
package com.twitter.aurora.scheduler.http;

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

import org.codehaus.jackson.annotate.JsonProperty;

import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.aurora.scheduler.storage.entities.IQuota;

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
      @Override public Response apply(StoreProvider storeProvider) {
        Map<String, IQuota> quotas;
        if (role == null) {
          quotas = storeProvider.getQuotaStore().fetchQuotas();
        } else {
          Optional<IQuota> quota = storeProvider.getQuotaStore().fetchQuota(role);
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

  private static final Function<IQuota, QuotaBean> TO_BEAN = new Function<IQuota, QuotaBean>() {
    @Override public QuotaBean apply(IQuota quota) {
      return new QuotaBean(quota.getNumCpus(), quota.getRamMb(), quota.getDiskMb());
    }
  };

  private static final class QuotaBean {
    private final double cpu;
    private final long ramMb;
    private final long diskMb;

    private QuotaBean(double cpu, long ramMb, long diskMb) {
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
