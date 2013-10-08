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

import java.io.StringWriter;
import java.util.Map;
import java.util.Map.Entry;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.FluentIterable;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.stats.ResourceCounter;
import com.twitter.aurora.scheduler.stats.ResourceCounter.GlobalMetric;
import com.twitter.aurora.scheduler.stats.ResourceCounter.Metric;
import com.twitter.aurora.scheduler.stats.ResourceCounter.MetricType;
import com.twitter.aurora.scheduler.storage.entities.ITaskConfig;
import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.util.templating.StringTemplateHelper;
import com.twitter.common.util.templating.StringTemplateHelper.TemplateException;

/**
 * A servlet to give an aggregate view of cluster resources consumed, grouped by category.
 */
@Path("/utilization")
public class Utilization {

  private final String clusterName;
  private final ResourceCounter counter;
  private final StringTemplateHelper templateHelper;

  @Inject
  Utilization(ResourceCounter counter, @ClusterName String clusterName) {
    templateHelper = new StringTemplateHelper(getClass(), "utilization", true);
    this.counter = Preconditions.checkNotNull(counter);
    this.clusterName = MorePreconditions.checkNotBlank(clusterName);
  }

  private String fillTemplate(Map<Display, Metric> metrics) {
    Function<Entry<Display, Metric>, DisplayMetric> transform =
        new Function<Entry<Display, Metric>, DisplayMetric>() {
          @Override public DisplayMetric apply(Entry<Display, Metric> entry) {
            return new DisplayMetric(entry.getKey(), entry.getValue());
          }
        };
    return fillTemplate(FluentIterable.from(metrics.entrySet()).transform(transform).toList());
  }

  private String fillTemplate(final Iterable<DisplayMetric> metrics) {
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
      return Objects.hashCode(title, link);
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof  Display)) {
        return false;
      }

      Display other = (Display) o;
      return Objects.equal(title, other.title) && Objects.equal(link, other.link);
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
  }

  private static final Function<GlobalMetric, DisplayMetric> TO_DISPLAY =
      new Function<GlobalMetric, DisplayMetric>() {
        @Override public DisplayMetric apply(GlobalMetric count) {
          return new DisplayMetric(
              new Display(
                  count.type.name().replace('_', ' ').toLowerCase(),
                  count.type.name().toLowerCase()),
              count);
        }
      };

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
    MetricType type = MetricType.valueOf(name.toUpperCase());
    if (type == null) {
      throw new WebApplicationException(
          Response.status(Status.BAD_REQUEST).entity("Invalid metric type.").build());
    }
    return type;
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

    Function<ITaskConfig, Display> toKey = new Function<ITaskConfig, Display>() {
      @Override public Display apply(ITaskConfig task) {
        String role = task.getOwner().getRole();
        return new Display(role, metric + "/" + role);
      }
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
    Function<ITaskConfig, Display> toKey = new Function<ITaskConfig, Display>() {
      @Override public Display apply(ITaskConfig task) {
        return new Display(task.getJobName(), null);
      }
    };
    Map<Display, Metric> byJob =
        counter.computeAggregates(Query.roleScoped(role).active(), type.filter, toKey);
    return Response.ok(fillTemplate(byJob)).build();
  }
}
