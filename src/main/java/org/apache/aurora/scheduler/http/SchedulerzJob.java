/**
 * Copyright 2013 Apache Software Foundation
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

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;
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
import com.twitter.common.base.Closure;

import org.antlr.stringtemplate.StringTemplate;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.filter.SchedulingFilter.Veto;
import org.apache.aurora.scheduler.metadata.NearestFit;
import org.apache.aurora.scheduler.state.CronJobManager;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.aurora.scheduler.storage.entities.ITaskConstraint;
import org.apache.aurora.scheduler.storage.entities.ITaskEvent;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

import static org.apache.aurora.gen.ScheduleStatus.ASSIGNED;
import static org.apache.aurora.gen.ScheduleStatus.FAILED;
import static org.apache.aurora.gen.ScheduleStatus.FINISHED;
import static org.apache.aurora.gen.ScheduleStatus.KILLED;
import static org.apache.aurora.gen.ScheduleStatus.KILLING;
import static org.apache.aurora.gen.ScheduleStatus.LOST;
import static org.apache.aurora.gen.ScheduleStatus.PENDING;
import static org.apache.aurora.gen.ScheduleStatus.RUNNING;
import static org.apache.aurora.gen.ScheduleStatus.SANDBOX_DELETED;
import static org.apache.aurora.gen.ScheduleStatus.STARTING;

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

  private static final Ordering<IScheduledTask> INSTANCE_ID_COMPARATOR =
    Ordering.natural().onResultOf(Tasks.SCHEDULED_TO_INSTANCE_ID);

  private static final Map<ScheduleStatus, Set<ScheduleStatus>> FILTER_MAP =
      ImmutableMap.<ScheduleStatus, Set<ScheduleStatus>>builder()
        .put(PENDING, EnumSet.of(PENDING))
        .put(RUNNING, EnumSet.of(ASSIGNED, STARTING, RUNNING, KILLING))
        // TODO(maxim): SANDBOX_DELETED can be issued for both FINISHED and FAILED states
        // Add a post-processing filtering to place it into the right bucket.
        .put(FINISHED, EnumSet.of(KILLED, FINISHED, SANDBOX_DELETED))
        .put(FAILED, EnumSet.of(LOST, FAILED))
      .build();

  private static final Function<Veto, String> GET_REASON = new Function<Veto, String>() {
    @Override
    public String apply(Veto veto) {
      return veto.getReason();
    }
  };

  // Double percents to escape formatting sequence.
  private static final String PORT_FORMAT = "%%port:%s%%";
  // TODO(William Farner): Search for usage of this, figure out a deprecation strategy to switch
  //                       to %instance_id%.
  private static final String INSTANCE_ID_REGEXP = "%shard_id%";
  private static final String TASK_ID_REGEXP = "%task_id%";
  private static final String HOST_REGEXP = "%host%";

  private static String expandText(String value, IAssignedTask task) {
    String expanded = value
        .replaceAll(INSTANCE_ID_REGEXP, String.valueOf(task.getInstanceId()))
        .replaceAll(TASK_ID_REGEXP, task.getTaskId());

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

  private final Function<IScheduledTask, Map<String, Object>> taskToStringMap =
      new Function<IScheduledTask, Map<String, Object>>() {
        @Override
        public Map<String, Object> apply(IScheduledTask scheduledTask) {
          final IAssignedTask task = scheduledTask.getAssignedTask();
          ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
            .put("taskId", task.getTaskId())
            .put("instanceId", task.getInstanceId())
            .put("slaveHost", task.isSetSlaveHost() ? task.getSlaveHost() : "")
            .put("taskEvents", scheduledTask.getTaskEvents());

          ITaskEvent statusEvent = Tasks.getLatestEvent(scheduledTask);

          if (scheduledTask.getStatus() == SANDBOX_DELETED) {
            // Avoid displaying SANDBOX_DELETED as completed status.
            statusEvent = Tasks.getSecondToLatestEvent(scheduledTask);
            builder.put("hideExecutorUri", true);
          } else if (scheduledTask.getStatus() == PENDING) {
            String pendingReason;
            Set<Veto> vetoes = nearestFit.getNearestFit(task.getTaskId());
            if (vetoes.isEmpty()) {
              pendingReason = "No matching hosts.";
            } else {
              pendingReason = Joiner.on(",").join(Iterables.transform(vetoes, GET_REASON));
            }
            builder.put("pendingReason", pendingReason);
          }

          builder.put("status", statusEvent.getStatus());
          builder.put("statusTimestamp", statusEvent.getTimestamp());

          Function<String, String> expander = new Function<String, String>() {
            @Override
            public String apply(String input) {
              return expandText(input, task);
            }
          };

          Map<String, String> links = ImmutableMap.of();
          if (apiConstants.LIVE_STATES.contains(scheduledTask.getStatus())) {
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
  private final CronJobManager cronJobManager;

  /**
   * Creates a new job servlet.
   *
   * @param storage a reference to scheduler storage.
   */
  @Inject
  public SchedulerzJob(
      Storage storage,
      CronJobManager cronJobManager,
      IServerInfo serverInfo,
      NearestFit nearestFit) {

    super("schedulerzjob");
    this.storage = checkNotNull(storage);
    this.clusterName = checkNotBlank(serverInfo.getClusterName());
    this.nearestFit = checkNotNull(nearestFit);
    this.cronJobManager = checkNotNull(cronJobManager);
  }

  private static <T> Iterable<T> offsetAndLimit(Iterable<T> iterable, int offset) {
    return ImmutableList.copyOf(Iterables.limit(Iterables.skip(iterable, offset), PAGE_SIZE));
  }

  private static String scaleMb(long mb) {
    return (mb >= 1024) ? ((mb / 1024) + " GiB") : (mb + " MiB");
  }

  private static final Function<IConstraint, String> DISPLAY_CONSTRAINT =
      new Function<IConstraint, String>() {
        @Override
        public String apply(IConstraint constraint) {
          StringBuilder sb = new StringBuilder().append(constraint.getName()).append(": ");
          ITaskConstraint taskConstraint = constraint.getConstraint();
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

  private static final Function<ITaskConfig, SchedulingDetails> CONFIG_TO_DETAILS =
      new Function<ITaskConfig, SchedulingDetails>() {
        @Override
        public SchedulingDetails apply(ITaskConfig task) {
          String resources = Joiner.on(", ").join(
              "cpu: " + task.getNumCpus(),
              "ram: " + scaleMb(task.getRamMb()),
              "disk: " + scaleMb(task.getDiskMb()));
          ImmutableMap.Builder<String, Object> details = ImmutableMap.<String, Object>builder()
              .put("resources", resources);
          if (!task.getConstraints().isEmpty()) {
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
          if (!task.getRequestedPorts().isEmpty()) {
            details.put("ports",
                Joiner.on(", ").join(ImmutableSortedSet.copyOf(task.getRequestedPorts())));
          }
          Optional<String> metadata = TransformationUtils.getMetadata(task);
          if (metadata.isPresent()) {
            details.put("metadata", metadata.get());
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
      Iterable<IAssignedTask> tasks) {

    Map<Integer, ITaskConfig> byInstance = Maps.transformValues(
        Maps.uniqueIndex(tasks, Tasks.ASSIGNED_TO_INSTANCE_ID),
        Tasks.ASSIGNED_TO_INFO);
    Map<Integer, SchedulingDetails> detailsByInstance =
        Maps.transformValues(byInstance, CONFIG_TO_DETAILS);
    Multimap<SchedulingDetails, Integer> instancesByDetails = Multimaps.invertFrom(
        Multimaps.forMap(detailsByInstance), HashMultimap.<SchedulingDetails, Integer>create());
    Map<SchedulingDetails, String> instanceStringsByDetails =
        Maps.transformValues(instancesByDetails.asMap(), TransformationUtils.INSTANCES_TOSTRING);
    return HashBiMap.create(instanceStringsByDetails).inverse();
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
      @Override
      public void execute(StringTemplate template) {
        template.setAttribute("cluster_name", clusterName);
        template.setAttribute(ADMIN_VIEW_PARAM, adminView != null);
        IJobKey jobKey = JobKeys.from(role, environment, job);

        boolean isCron = cronJobManager.hasJob(jobKey);
        template.setAttribute("is_cron", isCron);

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
        template.setAttribute("statsUrl", DisplayUtils.getJobDashboardUrl(jobKey));
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
          Set<IScheduledTask> activeTasks =
              Storage.Util.weaklyConsistentFetchTasks(storage, activeQuery.get());
          List<IScheduledTask> liveTasks = INSTANCE_ID_COMPARATOR.sortedCopy(activeTasks);
          template.setAttribute("activeTasks",
              ImmutableList.copyOf(
                  Iterables.transform(offsetAndLimit(liveTasks, offset), taskToStringMap)));
          hasMore = hasMore || (liveTasks.size() > (offset + PAGE_SIZE));
          template.setAttribute("schedulingDetails",
              buildSchedulingTable(Iterables.transform(liveTasks, Tasks.SCHEDULED_TO_ASSIGNED)));
        }
        if (completedQuery.isPresent()) {
          List<IScheduledTask> completedTasks = Lists.newArrayList(
              Storage.Util.weaklyConsistentFetchTasks(storage, completedQuery.get()));
          Collections.sort(completedTasks, Tasks.LATEST_ACTIVITY.reverse());
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
