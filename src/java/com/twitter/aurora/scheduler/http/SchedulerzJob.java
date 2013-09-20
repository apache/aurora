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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Constants;
import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.ScheduledTask;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskConstraint;
import com.twitter.aurora.gen.TaskEvent;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.base.Query;
import com.twitter.aurora.scheduler.base.Tasks;
import com.twitter.aurora.scheduler.filter.SchedulingFilter.Veto;
import com.twitter.aurora.scheduler.metadata.NearestFit;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.common.base.Closure;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.aurora.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.aurora.gen.ScheduleStatus.FAILED;
import static com.twitter.aurora.gen.ScheduleStatus.FINISHED;
import static com.twitter.aurora.gen.ScheduleStatus.KILLED;
import static com.twitter.aurora.gen.ScheduleStatus.KILLING;
import static com.twitter.aurora.gen.ScheduleStatus.LOST;
import static com.twitter.aurora.gen.ScheduleStatus.PENDING;
import static com.twitter.aurora.gen.ScheduleStatus.RUNNING;
import static com.twitter.aurora.gen.ScheduleStatus.STARTING;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * HTTP interface to view information about a job in the aurora scheduler.
 */
@Path("/scheduler/{role}/{environment}/{job}")
public class SchedulerzJob extends JerseyTemplateServlet {
  private static final String STATUS_FILTER_PARAM = "status";
  private static final String ADMIN_VIEW_PARAM = "admin";

  // Pagination controls.
  private static final String OFFSET_PARAM = "o";
  private static final int PAGE_SIZE = 50;

  private static final Ordering<ScheduledTask> SHARD_ID_COMPARATOR =
    Ordering.natural().onResultOf(Tasks.SCHEDULED_TO_SHARD_ID);

  private static final Map<ScheduleStatus, Set<ScheduleStatus>> FILTER_MAP =
      ImmutableMap.<ScheduleStatus, Set<ScheduleStatus>>builder()
        .put(PENDING, EnumSet.of(PENDING))
        .put(RUNNING, EnumSet.of(ASSIGNED, STARTING, RUNNING, KILLING))
        .put(FINISHED, EnumSet.of(KILLED, FINISHED))
        .put(FAILED, EnumSet.of(LOST, FAILED))
      .build();

  private static final Comparator<ScheduledTask> REVERSE_CHRON_COMPARATOR =
      new Comparator<ScheduledTask>() {
        @Override public int compare(ScheduledTask taskA, ScheduledTask taskB) {
          // Sort in reverse chronological order.
          Iterable<TaskEvent> taskAEvents = taskA.getTaskEvents();
          Iterable<TaskEvent> taskBEvents = taskB.getTaskEvents();

          boolean taskAHasEvents = taskAEvents != null && !Iterables.isEmpty(taskAEvents);
          boolean taskBHasEvents = taskBEvents != null && !Iterables.isEmpty(taskBEvents);
          if (taskAHasEvents && taskBHasEvents) {
            return Long.signum(Iterables.getLast(taskBEvents).getTimestamp()
                - Iterables.getLast(taskAEvents).getTimestamp());
          } else {
            return 0;
          }
        }
      };

  private static final Function<Veto, String> GET_REASON = new Function<Veto, String>() {
    @Override public String apply(Veto veto) {
      return veto.getReason();
    }
  };

  // Double percents to escape formatting sequence.
  private static final String PORT_FORMAT = "%%port:%s%%";
  private static final String SHARD_ID_REGEXP = "%shard_id%";
  private static final String TASK_ID_REGEXP = "%task_id%";
  private static final String HOST_REGEXP = "%host%";

  private static String expandText(String value, AssignedTask task) {
    String expanded = value;
    TaskConfig config = task.getTask();

    expanded = expanded.replaceAll(SHARD_ID_REGEXP, String.valueOf(config.getShardId()));
    expanded = expanded.replaceAll(TASK_ID_REGEXP, task.getTaskId());

    if (task.isSetSlaveHost()) {
      expanded = expanded.replaceAll(HOST_REGEXP, task.getSlaveHost());
    }

    // Expand ports.
    if (task.isSetAssignedPorts()) {
      for (Map.Entry<String, Integer> portEntry : task.getAssignedPorts().entrySet()) {
        expanded = expanded.replaceAll(
            String.format(PORT_FORMAT, portEntry.getKey()),
            String.valueOf(portEntry.getValue()));
      }
    }

    return expanded;
  }

  private final Function<ScheduledTask, Map<String, Object>> taskToStringMap =
      new Function<ScheduledTask, Map<String, Object>>() {
        @Override public Map<String, Object> apply(ScheduledTask scheduledTask) {
          final AssignedTask task = scheduledTask.getAssignedTask();
          ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("taskId", task.getTaskId())
            .put("shardId", task.getTask().getShardId())
            .put("slaveHost", task.isSetSlaveHost() ? task.getSlaveHost() : "")
            .put("status", scheduledTask.getStatus())
            .put("statusTimestamp", Iterables.getLast(scheduledTask.getTaskEvents()).getTimestamp())
            .put("taskEvents", scheduledTask.getTaskEvents());

          if (scheduledTask.getStatus() == ScheduleStatus.PENDING) {
            String pendingReason;
            Set<Veto> vetoes = nearestFit.getNearestFit(task.getTaskId());
            if (vetoes.isEmpty()) {
              pendingReason = "No matching hosts.";
            } else {
              pendingReason = Joiner.on(",").join(Iterables.transform(vetoes, GET_REASON));
            }
            builder.put("pendingReason", pendingReason);
          }

          Function<String, String> expander = new Function<String, String>() {
            @Override public String apply(String input) {
              return expandText(input, task);
            }
          };

          Map<String, String> links = ImmutableMap.of();
          if (Constants.LIVE_STATES.contains(scheduledTask.getStatus())) {
            links =
                ImmutableMap.copyOf(Maps.transformValues(task.getTask().getTaskLinks(), expander));
          }
          builder.put("links", links);
          builder.put("executorPort", 1338);
          if (task.isSetSlaveHost()) {
            builder.put("executorUri",
                "http://" + task.getSlaveHost() + ":1338/task/" + task.getTaskId());
          }
          return builder.build();
        }
      };

  private final Storage storage;
  private final String clusterName;
  private final NearestFit nearestFit;

  /**
   * Creates a new job servlet.
   *
   * @param storage Backing store to fetch tasks from.
   * @param clusterName Name of the serving cluster.
   */
  @Inject
  public SchedulerzJob(
      Storage storage,
      @ClusterName String clusterName,
      NearestFit nearestFit) {

    super("schedulerzjob");
    this.storage = checkNotNull(storage);
    this.clusterName = checkNotBlank(clusterName);
    this.nearestFit = checkNotNull(nearestFit);
  }

  private static <T> Iterable<T> offsetAndLimit(Iterable<T> iterable, int offset) {
    return ImmutableList.copyOf(Iterables.limit(Iterables.skip(iterable, offset), PAGE_SIZE));
  }

  private static String scaleMb(long mb) {
    return (mb >= 1024) ? ((mb / 1024) + " GiB") : (mb + " MiB");
  }

  private static final Function<Constraint, String> DISPLAY_CONSTRAINT =
      new Function<Constraint, String>() {
        @Override public String apply(Constraint constraint) {
          StringBuilder sb = new StringBuilder().append(constraint.getName()).append(": ");
          TaskConstraint taskConstraint = constraint.getConstraint();
          switch (taskConstraint.getSetField()) {
            case VALUE:
              if (taskConstraint.getValue().isNegated()) {
                sb.append("not ");
              }
              sb.append(Joiner.on(", ").join(taskConstraint.getValue().getValues()));
              break;

            case LIMIT:
              sb.append("limit ").append(taskConstraint.getLimit().getLimit());
              break;

            default:
              sb.append("Unhandled constraint type " + taskConstraint.getSetField());
          }

          return sb.toString();
        }
      };

  private static final Function<TaskConfig, SchedulingDetails> CONFIG_TO_DETAILS =
      new Function<TaskConfig, SchedulingDetails>() {
        @Override public SchedulingDetails apply(TaskConfig task) {
          String resources = Joiner.on(", ").join(
              "cpu: " + task.getNumCpus(),
              "ram: " + scaleMb(task.getRamMb()),
              "disk: " + scaleMb(task.getDiskMb()));
          ImmutableMap.Builder<String, Object> details = ImmutableMap.<String, Object>builder()
              .put("resources", resources);
          if (task.getConstraintsSize() > 0) {
            Iterable<String> displayConstraints = FluentIterable.from(task.getConstraints())
                .transform(DISPLAY_CONSTRAINT)
                .toSortedList(Ordering.<String>natural());
            details.put("constraints", Joiner.on(", ").join(displayConstraints));
          }
          if (task.isIsService()) {
            details.put("service", "true");
          }
          if (task.isProduction()) {
            details.put("production", "true");
          }
          if (task.getRequestedPortsSize() > 0) {
            details.put("ports",
                Joiner.on(", ").join(ImmutableSortedSet.copyOf(task.getRequestedPorts())));
          }
          if (task.getPackagesSize() > 0) {
            List<String> packages = Ordering.natural().sortedCopy(
                Iterables.transform(task.getPackages(), TransformationUtils.PACKAGE_TOSTRING));
            details.put(
                "packages",
                Joiner.on(',').join(packages));
          }
          details.put("contact", task.isSetContactEmail() ? task.getContactEmail() : "none");
          return new SchedulingDetails(details.build());
        }
      };

  static class SchedulingDetails {
    private final Map<String, Object> details;

    SchedulingDetails(ImmutableMap<String, Object> details) {
      this.details = details;
    }

    public Map<String, Object> getDetails() {
      return details;
    }

    @Override
    public int hashCode() {
      return details.hashCode();
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SchedulingDetails)) {
        return false;
      }

      SchedulingDetails other = (SchedulingDetails) o;
      return other.details.equals(details);
    }
  }



  private static Map<String, SchedulingDetails> buildSchedulingTable(
      Iterable<TaskConfig> tasks) {

    Map<Integer, TaskConfig> byShard = Maps.uniqueIndex(tasks, Tasks.INFO_TO_SHARD_ID);
    Map<Integer, SchedulingDetails> detailsByShard =
        Maps.transformValues(byShard, CONFIG_TO_DETAILS);
    Multimap<SchedulingDetails, Integer> shardsByDetails = Multimaps.invertFrom(
        Multimaps.forMap(detailsByShard), HashMultimap.<SchedulingDetails, Integer>create());
    Map<SchedulingDetails, String> shardStringsByDetails =
        Maps.transformValues(shardsByDetails.asMap(), TransformationUtils.SHARDS_TOSTRING);
    return HashBiMap.create(shardStringsByDetails).inverse();
  }

  /**
   * Fetches the landing page for a job within a role.
   *
   * @return HTTP response.
   */
  @GET
  @Produces(MediaType.TEXT_HTML)
  public Response get(
      @PathParam("role") final String role,
      @PathParam("environment") final String environment,
      @PathParam("job") final String job,
      @QueryParam(OFFSET_PARAM) final int offset,
      @QueryParam(STATUS_FILTER_PARAM) final String filterArg,
      @QueryParam(ADMIN_VIEW_PARAM) final String adminView) {

    return fillTemplate(new Closure<StringTemplate>() {
      @Override public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);
        template.setAttribute(ADMIN_VIEW_PARAM, adminView != null);

        ScheduleStatus statusFilter = null;
        if (filterArg != null) {
          template.setAttribute(STATUS_FILTER_PARAM, filterArg);

          try {
            statusFilter = ScheduleStatus.valueOf(filterArg.toUpperCase());
          } catch (IllegalArgumentException e) {
            template.setAttribute("exception", "Invalid status type: " + filterArg);
            return;
          }
        }

        template.setAttribute("role", role);
        template.setAttribute("environment", environment);
        template.setAttribute("job", job);
        template.setAttribute("statsUrl", DisplayUtils.getJobDashboardUrl(role, environment, job));
        boolean hasMore = false;

        Query.Builder builder = Query.jobScoped(JobKeys.from(role, environment, job));

        Optional<Query.Builder> activeQuery = Optional.absent();
        Optional<Query.Builder> completedQuery = Optional.absent();
        if (statusFilter != null) {
          Collection<ScheduleStatus> queryStatuses = FILTER_MAP.get(statusFilter);
          if (Tasks.isActive(statusFilter)) {
            activeQuery = Optional.of(builder.byStatus(queryStatuses));
          } else {
            completedQuery = Optional.of(builder.byStatus(queryStatuses));
          }
        } else {
          activeQuery = Optional.of(builder.active());
          completedQuery = Optional.of(builder.terminal());
        }

        if (activeQuery.isPresent()) {
          Set<ScheduledTask> activeTasks =
              Storage.Util.weaklyConsistentFetchTasks(storage, activeQuery.get());
          List<ScheduledTask> liveTasks = SHARD_ID_COMPARATOR.sortedCopy(activeTasks);
          template.setAttribute("activeTasks",
              ImmutableList.copyOf(
                  Iterables.transform(offsetAndLimit(liveTasks, offset), taskToStringMap)));
          hasMore = hasMore || (liveTasks.size() > (offset + PAGE_SIZE));
          template.setAttribute("schedulingDetails",
              buildSchedulingTable(Iterables.transform(liveTasks, Tasks.SCHEDULED_TO_INFO)));
        }
        if (completedQuery.isPresent()) {
          List<ScheduledTask> completedTasks = Lists.newArrayList(
              Storage.Util.weaklyConsistentFetchTasks(storage, completedQuery.get()));
          Collections.sort(completedTasks, REVERSE_CHRON_COMPARATOR);
          template.setAttribute("completedTasks",
              ImmutableList.copyOf(
                  Iterables.transform(offsetAndLimit(completedTasks, offset), taskToStringMap)));
          hasMore = hasMore || (completedTasks.size() > (offset + PAGE_SIZE));
        }

        template.setAttribute("offset", offset);
        if (offset > 0) {
          template.setAttribute("prevOffset", Math.max(0, offset - PAGE_SIZE));
        }
        if (hasMore) {
          template.setAttribute("nextOffset", offset + PAGE_SIZE);
        }
      }
    });
  }
}
