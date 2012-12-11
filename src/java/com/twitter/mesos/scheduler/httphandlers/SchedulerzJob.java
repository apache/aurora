package com.twitter.mesos.scheduler.httphandlers;

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
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Range;
import com.google.inject.Inject;

import org.antlr.stringtemplate.StringTemplate;

import com.twitter.common.base.Closure;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Constants;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.Package;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.CommandLineExpander;
import com.twitter.mesos.scheduler.Numbers;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;
import com.twitter.mesos.scheduler.metadata.NearestFit;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.FAILED;
import static com.twitter.mesos.gen.ScheduleStatus.FINISHED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLING;
import static com.twitter.mesos.gen.ScheduleStatus.LOST;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RUNNING;
import static com.twitter.mesos.gen.ScheduleStatus.STARTING;

/**
 * HTTP interface to view information about a job in the mesos scheduler.
 */
@Path("/scheduler/{role}/{job}")
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
              return CommandLineExpander.expand(input, task);
            }
          };

          Map<String, String> links = ImmutableMap.of();
          if (Constants.LIVE_STATES.contains(scheduledTask.getStatus())) {
            links =
                ImmutableMap.copyOf(Maps.transformValues(task.getTask().getTaskLinks(), expander));
          }
          builder.put("links", links);

          if (Tasks.isThermos(task.getTask())) {
            builder.put("executorPort", 1338);
            if (task.isSetSlaveHost()) {
              builder.put("executorUri",
                  "http://" + task.getSlaveHost() + ":1338/task/" + task.getTaskId());
            }
          } else {
            builder.put("executorPort", 1337);
            if (task.isSetSlaveHost()) {
              builder.put("executorUri",
                  "http://" + task.getSlaveHost() + ":1337/task?task=" + task.getTaskId());
            }
          }

          return builder.build();
        }
      };

  private final SchedulerCore scheduler;
  private final String clusterName;
  private final NearestFit nearestFit;

  /**
   * Creates a new job servlet.
   *
   * @param scheduler Core scheduler.
   * @param clusterName Name of the serving cluster.
   */
  @Inject
  public SchedulerzJob(
      SchedulerCore scheduler,
      @ClusterName String clusterName,
      NearestFit nearestFit) {

    super("schedulerzjob");
    this.scheduler = checkNotNull(scheduler);
    this.clusterName = checkNotBlank(clusterName);
    this.nearestFit = checkNotNull(nearestFit);
  }

  private static <T> Iterable<T> offsetAndLimit(Iterable<T> iterable, int offset) {
    return ImmutableList.copyOf(Iterables.limit(Iterables.skip(iterable, offset), PAGE_SIZE));
  }

  private static String scaleMb(long mb) {
    return (mb >= 1024) ? ((mb / 1024) + " GiB") : (mb + " MiB");
  }

  private static String humanReadableConstraint(TaskConstraint constraint) {
    StringBuilder sb = new StringBuilder();
    switch (constraint.getSetField()) {
      case VALUE:
        if (constraint.getValue().isNegated()) {
          sb.append("not ");
        }
        sb.append(Joiner.on(", ").join(constraint.getValue().getValues()));
        break;

      case LIMIT:
        sb.append("limit ").append(constraint.getLimit().getLimit());
        break;

      default:
        sb.append("Unhandled constraint type " + constraint.getSetField());
    }

    return sb.toString();
  }

  private static final Function<TwitterTaskInfo, SchedulingDetails> CONFIG_TO_DETAILS =
      new Function<TwitterTaskInfo, SchedulingDetails>() {
        @Override public SchedulingDetails apply(TwitterTaskInfo task) {
          ImmutableMap.Builder<String, Object> details = ImmutableMap.<String, Object>builder()
              .put("contact", task.isSetContactEmail() ? task.getContactEmail() : "none")
              .put("CPU", task.getNumCpus())
              .put("RAM" , scaleMb(task.getRamMb()))
              .put("disk", scaleMb(task.getDiskMb()));
          if (task.isProduction()) {
            details.put("production", "true");
          }
          if (task.getRequestedPortsSize() > 0) {
            details.put("ports", Joiner.on(",").join(task.getRequestedPorts()));
          }
          for (Constraint constraint : task.getConstraints()) {
            details.put(constraint.getName(), humanReadableConstraint(constraint.getConstraint()));
          }
          if (task.getPackagesSize() > 0) {
            details.put(
                "packages",
                Joiner.on(',').join(Iterables.transform(task.getPackages(), PACKAGE_TOSTRING)));
          }
          return new SchedulingDetails(details.build());
        }
      };

  static class SchedulingDetails {
    final Map<String, Object> details;

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

  private static final Function<Package, String> PACKAGE_TOSTRING =
      new Function<Package, String>() {
        @Override public String apply(Package pkg) {
          return pkg.getRole() + "/" + pkg.getName() + " v" + pkg.getVersion();
        }
      };

  private static final Function<Range<Integer>, String> RANGE_TOSTRING =
      new Function<Range<Integer>, String>() {
        @Override public String apply(Range<Integer> range) {
          int lower = range.lowerEndpoint();
          int upper = range.upperEndpoint();
          return (lower == upper) ? String.valueOf(lower) : (lower + " - " + upper);
        }
      };

  private static final Function<Collection<Integer>, String> SHARDS_TOSTRING =
      new Function<Collection<Integer>, String>() {
        @Override public String apply(Collection<Integer> shards) {
          return Joiner.on(", ")
              .join(Iterables.transform(Numbers.toRanges(shards), RANGE_TOSTRING));
        }
      };

  private static Map<String, SchedulingDetails> buildSchedulingTable(
      Iterable<TwitterTaskInfo> tasks) {

    Map<Integer, TwitterTaskInfo> byShard = Maps.uniqueIndex(tasks, Tasks.INFO_TO_SHARD_ID);
    Map<Integer, SchedulingDetails> detailsByShard =
        Maps.transformValues(byShard, CONFIG_TO_DETAILS);
    Multimap<SchedulingDetails, Integer> shardsByDetails = Multimaps.invertFrom(
        Multimaps.forMap(detailsByShard), HashMultimap.<SchedulingDetails, Integer>create());
    Map<SchedulingDetails, String> shardStringsByDetails =
        Maps.transformValues(shardsByDetails.asMap(), SHARDS_TOSTRING);
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
        template.setAttribute("job", job);

        TaskQuery query = new TaskQuery()
            .setOwner(new Identity().setRole(role))
            .setJobName(job);

        boolean hasMore = false;

        Optional<TaskQuery> activeQuery = Optional.absent();
        Optional<TaskQuery> completedQuery = Optional.absent();
        if (statusFilter != null) {
          query.setStatuses(FILTER_MAP.get(statusFilter));
          if (Tasks.isActive(statusFilter)) {
            activeQuery = Optional.of(query);
          } else {
            completedQuery = Optional.of(query);
          }
        } else {
          activeQuery = Optional.of(new TaskQuery(query).setStatuses(Tasks.ACTIVE_STATES));
          completedQuery = Optional.of(new TaskQuery(query).setStatuses(Tasks.TERMINAL_STATES));
        }

        if (activeQuery.isPresent()) {
          Set<ScheduledTask> activeTasks = scheduler.getTasks(activeQuery.get());
          List<ScheduledTask> liveTasks = SHARD_ID_COMPARATOR.sortedCopy(activeTasks);
          template.setAttribute("activeTasks",
              ImmutableList.copyOf(
                  Iterables.transform(offsetAndLimit(liveTasks, offset), taskToStringMap)));
          hasMore = hasMore || (liveTasks.size() > (offset + PAGE_SIZE));
          template.setAttribute("schedulingDetails",
              buildSchedulingTable(Iterables.transform(liveTasks, Tasks.SCHEDULED_TO_INFO)));
        }
        if (completedQuery.isPresent()) {
          List<ScheduledTask> completedTasks =
              Lists.newArrayList(scheduler.getTasks(completedQuery.get()));
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
