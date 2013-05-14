package com.twitter.mesos.scheduler.http;

import java.util.Map;

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
import com.google.inject.Inject;

import org.codehaus.jackson.annotate.JsonProperty;

import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

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
    return storage.doInTransaction(new Work.Quiet<Response>() {
      @Override
      public Response apply(StoreProvider storeProvider) {
        Map<String, Quota> quotas;
        if (role == null) {
          quotas = storeProvider.getQuotaStore().fetchQuotas();
        } else {
          Optional<Quota> quota = storeProvider.getQuotaStore().fetchQuota(role);
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

  private static final Function<Quota, QuotaBean> TO_BEAN = new Function<Quota, QuotaBean>() {
    @Override public QuotaBean apply(Quota quota) {
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
