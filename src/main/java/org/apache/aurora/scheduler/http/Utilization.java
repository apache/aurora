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

import java.io.StringWriter;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.util.templating.StringTemplateHelper;
import org.apache.aurora.common.util.templating.StringTemplateHelper.TemplateException;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.stats.ResourceCounter;
import org.apache.aurora.scheduler.stats.ResourceCounter.Metric;
import org.apache.aurora.scheduler.stats.ResourceCounter.MetricType;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

/**
 * A servlet to give an aggregate view of cluster resources consumed, grouped by category.
 */
@Path("/utilization")
public class Utilization {

  private final String clusterName;
  private final ResourceCounter counter;
  private final StringTemplateHelper templateHelper;

  @Inject
  Utilization(ResourceCounter counter, IServerInfo serverInfo) {
    templateHelper = new StringTemplateHelper(getClass(), "utilization", true);
    this.counter = Objects.requireNonNull(counter);
    this.clusterName = MorePreconditions.checkNotBlank(serverInfo.getClusterName());
  }

  private String fillTemplate(Map<Display, Metric> metrics) {
    Function<Entry<Display, Metric>, DisplayMetric> transform =
        entry -> new DisplayMetric(entry.getKey(), entry.getValue());
    return fillTemplate(FluentIterable.from(metrics.entrySet()).transform(transform).toList());
  }

  private String fillTemplate(final Iterable<DisplayMetric> metrics) {
    StringWriter output = new StringWriter();
    try {
      templateHelper.writeTemplate(output, template -> {
        template.setAttribute("cluster_name", clusterName);
        template.setAttribute("metrics", metrics);
      });
    } catch (TemplateException e) {
      throw new WebApplicationException(e);
    }
    return output.toString();
  }

  private static class Display {
    private final String title;
    @Nullable
    private final String link;

    Display(String title, @Nullable String link) {
      this.title = title;
      this.link = link;
    }

    @Override
    public int hashCode() {
      return Objects.hash(title, link);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof  Display)) {
        return false;
      }

      Display other = (Display) o;
      return Objects.equals(title, other.title) && Objects.equals(link, other.link);
    }
  }

  private static class DisplayMetric extends Metric {
    private final Display display;

    DisplayMetric(Display display, Metric wrapped) {
      super(wrapped);
      this.display = display;
    }

    public String getTitle() {
      return display.title;
    }

    @Nullable
    public String getLink() {
      return display.link;
    }

    public long getCpu() {
      return valueOf(ResourceType.CPUS);
    }

    public long getRam() {
      return valueOf(ResourceType.RAM_MB);
    }

    public long getDisk() {
      return valueOf(ResourceType.DISK_MB);
    }

    private long valueOf(ResourceType type) {
      return (long) getBag().valueOf(type);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof DisplayMetric)) {
        return false;
      }

      DisplayMetric other = (DisplayMetric) o;

      return super.equals(o)
          && display.equals(other.display);
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), display);
    }
  }

  private static final Function<Metric, DisplayMetric> TO_DISPLAY =
      count -> new DisplayMetric(
          new Display(
              count.type.name().replace('_', ' ').toLowerCase(),
              count.type.name().toLowerCase()),
          count);

  /**
   * Displays the aggregate utilization for the entire cluster.
   *
   * @return HTML-formatted cluster utilization.
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response aggregateCluster() {
    Iterable<DisplayMetric> metrics =
        FluentIterable.from(counter.computeConsumptionTotals()).transform(TO_DISPLAY).toList();
    return Response.ok(fillTemplate(metrics)).build();
  }

  private MetricType getTypeByName(String name) throws WebApplicationException {
    try {
      return MetricType.valueOf(name.toUpperCase(Locale.ENGLISH));
    } catch (IllegalArgumentException e) {
      throw new WebApplicationException(
          e,
          Response.status(Status.BAD_REQUEST).entity("Invalid metric type.").build());
    }
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
  public Response aggregateRoles(@PathParam("metric") final String metric) {
    final MetricType type = getTypeByName(metric);

    Function<ITaskConfig, Display> toKey = task -> {
      String role = task.getJob().getRole();
      return new Display(role, metric + "/" + role);
    };
    Map<Display, Metric> byRole =
        counter.computeAggregates(Query.unscoped().active(), type.filter, toKey);
    return Response.ok(fillTemplate(byRole)).build();
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

    MetricType type = getTypeByName(metric);
    Function<ITaskConfig, Display> toKey = task -> new Display(task.getJob().getName(), null);
    Map<Display, Metric> byJob =
        counter.computeAggregates(Query.roleScoped(role).active(), type.filter, toKey);
    return Response.ok(fillTemplate(byJob)).build();
  }
}
