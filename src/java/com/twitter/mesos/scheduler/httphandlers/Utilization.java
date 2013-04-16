package com.twitter.mesos.scheduler.httphandlers;

import java.io.StringWriter;
import java.util.Arrays;
import java.util.List;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.collections.Pair;
import com.twitter.common.util.templating.StringTemplateHelper;
import com.twitter.common.util.templating.StringTemplateHelper.TemplateException;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.Storage;

/**
 * A servlet to give an aggregate view of cluster resources consumed, grouped by category.
 */
@Path("/utilization")
public class Utilization {

  private static final TaskQuery ALL_ACTIVE = Query.byStatus(Tasks.ACTIVE_STATES);

  private final Storage storage;
  private final String clusterName;
  private final StringTemplateHelper templateHelper;

  @Inject
  Utilization(Storage storage, @ClusterName String clusterName) {
    templateHelper = new StringTemplateHelper(getClass(), "utilization", true);
    this.storage = Preconditions.checkNotNull(storage);
    this.clusterName = MorePreconditions.checkNotBlank(clusterName);
  }

  private Iterable<TwitterTaskInfo> getTasks(TaskQuery query) {
    return Iterables.transform(Storage.Util.fetchTasks(storage, query), Tasks.SCHEDULED_TO_INFO);
  }

  private String fillTemplate(final Iterable<? extends Metric> metrics) {
    StringWriter output = new StringWriter();
    try {
      templateHelper.writeTemplate(output, new Closure<StringTemplate>() {
        @Override public void execute(StringTemplate template) {
          template.setAttribute("cluster_name", clusterName);
          template.setAttribute("metrics", metrics);
        }
      });
    } catch (TemplateException e) {
      throw new WebApplicationException(e);
    }
    return output.toString();
  }

  /**
   * Displays the aggregate utilization for the entire cluster.
   *
   * @return HTML-formatted cluster utilization.
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response aggregateCluster() {
    List<ResourceMetric> metrics = Arrays.asList(
        new ResourceMetric("Total consumed", MetricType.TOTAL_CONSUMED),
        new ResourceMetric("Dedicated consumed", MetricType.DEDICATED_CONSUMED),
        new ResourceMetric("Quota consumed", MetricType.QUOTA_CONSUMED),
        new ResourceMetric("Free pool consumed", MetricType.FREE_POOL_CONSUMED));

    for (TwitterTaskInfo task : getTasks(ALL_ACTIVE)) {
      for (ResourceMetric metric : metrics) {
        metric.maybeAccumulate(task);
      }
    }

    return Response.ok(fillTemplate(metrics)).build();
  }

  private static LoadingCache<Pair<String, String>, Metric> newMetricCache() {
    return CacheBuilder.newBuilder()
        .build(new CacheLoader<Pair<String, String>, Metric>() {
          @Override public Metric load(Pair<String, String> key) {
            return new Metric(key.getFirst(), key.getSecond());
          }
        });
  }

  /**
   * Displays the aggregate utilization for roles within a metric type.
   *
   * @param metric Metric id.
   * @return HTML-formatted utilization within the metric type.
   */
  @GET
  @Path("/{metric}")
  @Produces(MediaType.TEXT_HTML)
  public Response aggregateRoles(@PathParam("metric") String metric) {
    MetricType type = MetricType.getById(metric);

    LoadingCache<Pair<String, String>, Metric> byRole = newMetricCache();
    for (TwitterTaskInfo task : Iterables.filter(getTasks(ALL_ACTIVE), type.filter)) {
      String role = task.getOwner().getRole();
      Metric m = byRole.getUnchecked(Pair.of(role, metric + "/" + role));
      m.accumulate(task);
    }

    return Response.ok(fillTemplate(byRole.asMap().values())).build();
  }

  /**
   * Displays the aggregate utilization for jobs within a role.
   *
   * @param metric Metric id.
   * @param role Role for jobs to aggregate.
   * @return HTML-formatted utilization within the metric/role.
   */
  @GET
  @Path("/{metric}/{role}")
  @Produces(MediaType.TEXT_HTML)
  public Response aggregateJobs(
      @PathParam("metric") String metric,
      @PathParam("role") String role) {

    MetricType type = MetricType.getById(metric);
    LoadingCache<Pair<String, String>, Metric> byJob = newMetricCache();
    TaskQuery query = Query.roleScoped(role).active().get();
    for (TwitterTaskInfo task : Iterables.filter(getTasks(query), type.filter)) {
      byJob.getUnchecked(Pair.<String, String>of(task.getJobName(), null)).accumulate(task);
    }

    return Response.ok(fillTemplate(byJob.asMap().values())).build();
  }

  private enum MetricType {
    TOTAL_CONSUMED("total", Predicates.<TwitterTaskInfo>alwaysTrue()),
    DEDICATED_CONSUMED("dedicated", new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(@Nullable TwitterTaskInfo task) {
        return ConfigurationManager.isDedicated(task);
      }
    }),
    QUOTA_CONSUMED("quota", new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(TwitterTaskInfo task) {
        return task.isProduction();
      }
    }),
    FREE_POOL_CONSUMED("freepool", new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(TwitterTaskInfo task) {
        return !ConfigurationManager.isDedicated(task) && !task.isProduction();
      }
    });

    final String id;
    final Predicate<TwitterTaskInfo> filter;

    MetricType(String id, Predicate<TwitterTaskInfo> filter) {
      this.id = id;
      this.filter = filter;
    }

    static MetricType getById(String id) {
      for (MetricType type : MetricType.values()) {
        if (type.id.equals(id)) {
          return type;
        }
      }

      throw new WebApplicationException(
          Response.status(Status.BAD_REQUEST).entity("Invalid metric type.").build());
    }
  }

  private static class ResourceMetric extends Metric {
    final MetricType type;

    ResourceMetric(String title, MetricType type) {
      super(title, type.id);
      this.type = type;
    }

    void maybeAccumulate(TwitterTaskInfo task) {
      if (type.filter.apply(task)) {
        accumulate(task);
      }
    }
  }

  private static class Metric {
    final String title;
    @Nullable
    String link;
    long cpu;
    long ramMb;
    long diskMb;

    Metric(String title, @Nullable String link) {
      this.title = title;
      this.link = link;
      this.cpu = 0;
      this.ramMb = 0;
      this.diskMb = 0;
    }

    Metric(String title) {
      this(title, null);
    }

    void accumulate(TwitterTaskInfo task) {
      cpu += task.numCpus;
      ramMb += task.ramMb;
      diskMb += task.diskMb;
    }

    public String getTitle() {
      return title;
    }

    @Nullable
    public String getLink() {
      return link;
    }

    public long getCpu() {
      return cpu;
    }

    public long getRamGb() {
      return ramMb / 1024;
    }

    public long getDiskGb() {
      return diskMb / 1024;
    }
  }
}
