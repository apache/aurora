package com.twitter.mesos.updater;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.gen.LiveTask;
import com.twitter.mesos.gen.MesosSchedulerManager.Iface;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduleStatusResponse;
import com.twitter.mesos.gen.ShardUpdateRequest;
import com.twitter.mesos.gen.ShardUpdateResponse;
import com.twitter.mesos.gen.TaskEvent;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.UpdateConfig;
import com.twitter.mesos.gen.UpdateConfigResponse;
import com.twitter.mesos.updater.ConfigParser.UpdateConfigException;
import com.twitter.mesos.updater.UpdateLogic.UpdateException;

import org.apache.thrift.TException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.gen.ScheduleStatus.*;

/**
 * Coordinates between the scheduler and the update logic to perform an update.
 *
 * @author wfarner
 */
public class Coordinator {

  private static final Logger LOG = Logger.getLogger(Coordinator.class.getName());

  private final Iface scheduler;
  private final String updateToken;

  @Inject
  public Coordinator(Iface scheduler, @UpdateToken String updateToken) {
    this.scheduler = checkNotNull(scheduler);
    this.updateToken = checkNotBlank(updateToken);
  }

  public boolean run() throws TException, UpdateConfigException, InterruptedException {
    try {
      UpdateConfigResponse response = scheduler.getUpdateConfig(updateToken);
      if (response.getResponseCode() != ResponseCode.OK) {
        LOG.severe("Failed to fetch update config from scheduler, message: "
                   + response.getMessage());
        return false;
      }

      LOG.info("Fetched update configuration from scheduler: " + response);

      UpdateConfig config = response.getConfig();
      config = config == null ? new UpdateConfig() : config;
      ConfigParser.parseAndVerify(config);
      Set<Integer> oldShards = checkNotBlank(response.getOldShards());
      Set<Integer> newShards = checkNotBlank(response.getNewShards());

      return new UpdateLogic(newShards, oldShards, config,
          new ExceptionalFunction<UpdateCommand, Integer, UpdateException>() {
            @Override public Integer apply(UpdateCommand command) throws UpdateException {
              return updateAndWatch(command);
            }
          }).run();
    } finally {
      LOG.info("Attempting to mark update as completed with scheduler.");
      try {
        scheduler.finishUpdate(updateToken);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Failed to inform scheduler of finished update.", e);
      }
    }
  }

  private int updateAndWatch(UpdateCommand command) throws UpdateException {
    ShardUpdateRequest request = new ShardUpdateRequest()
        .setUpdateToken(updateToken)
        .setRestartShards(command.shardIds)
        .setRollback(command.type == UpdateCommand.Type.ROLLBACK_TASK);

    LOG.info("Asking scheduler to restart shards " + command.shardIds);
    ShardUpdateResponse response;
    try {
      response = scheduler.updateShards(request);
    } catch (TException e) {
      throw new UpdateException("Failed to send shard update " + request, e);
    }

    long restartedAtTimestamp = System.currentTimeMillis();

    if (response.getResponseCode() != ResponseCode.OK) {
      throw new UpdateException("Scheduler rejected shard restart request, msg: "
                                + response.getMessage());
    }

    LOG.info("Restart initiated successfully, watching tasks for "
             + command.updateWatchSecs + " seconds.");

    Set<String> restartedTaskIds = response.getRestartedTaskIds();

    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new UpdateException("Interrupted while watching tasks.");
      }

      // We are finished when we've reached the failure limit, or all tasks have been RUNNING for
      // the watch period.
      LOG.info("Getting status of tasks " + restartedTaskIds);
      Map<String, TaskEvent> latestEvents;
      try {
        latestEvents = getStatus(restartedTaskIds);
      } catch (TException e) {
        throw new UpdateException("Failed to get status of " + restartedTaskIds, e);
      }

      int failures = 0;

      int missingTasks = Sets.difference(restartedTaskIds, latestEvents.keySet()).size();
      failures += missingTasks;
      LOG.info("Missing tasks: " + missingTasks);

      int completedTasks = Iterables.size(Iterables.filter(latestEvents.values(), IS_FAILED_EVENT));
      failures += completedTasks;
      LOG.info("Terminatd tasks: " + completedTasks);

      int startTimedOutTasks = Iterables.size(Iterables.filter(latestEvents.values(),
          exceedingRestartTimeout(restartedAtTimestamp, command.restartTimeoutSecs)));
      failures += startTimedOutTasks;
      LOG.info("Tasks that failed to start promptly: " + startTimedOutTasks);

      if (failures > command.allowedFailures) return failures;

      int successes = Iterables.size(Iterables.filter(latestEvents.values(),
          runningForAtLeast(command.updateWatchSecs)));
      LOG.info("Tasks that survived watch period: " + successes);

      // If we have made a decision for all tasks, return the failure count.
      if ((successes + failures) == restartedTaskIds.size()) return failures;
    }
  }

  /**
   * Terminal states, which a task should not move from.
   */
  private static final Set<ScheduleStatus> TERMINAL_STATES = EnumSet.of(FAILED, FINISHED, KILLED, KILLED_BY_CLIENT, LOST, NOT_FOUND);
  private static final Predicate<TaskEvent> IS_FAILED_EVENT = new Predicate<TaskEvent>() {
    @Override public boolean apply(TaskEvent event) {
      return TERMINAL_STATES.contains(event.getStatus());
    }
  };

  private static final Set<ScheduleStatus> STARTING_STATES = ImmutableSet.of(PENDING, STARTING);
  private static Predicate<TaskEvent> exceedingRestartTimeout(final long restartedAtTimestamp,
      final int restartTimeoutSecs) {
    return new Predicate<TaskEvent>() {
      @Override public boolean apply(TaskEvent event) {
        long secsSinceRestart = Amount.of(System.currentTimeMillis() - restartedAtTimestamp,
            Time.MILLISECONDS).as(Time.SECONDS);

        return STARTING_STATES.contains(event.getStatus())
               && secsSinceRestart > restartTimeoutSecs;
      }
    };
  }

  private static Predicate<TaskEvent> runningForAtLeast(final long minRunPeriodSecs) {
    return new Predicate<TaskEvent>() {
      @Override public boolean apply(TaskEvent event) {
        long eventDurationSecs = Amount.of(System.currentTimeMillis() - event.getTimestamp(),
            Time.MILLISECONDS).as(Time.SECONDS);

        return event.getStatus() == RUNNING && eventDurationSecs >= minRunPeriodSecs;
      }
    };
  }

  private Map<String, TaskEvent> getStatus(Set<String> taskIds) throws TException {
    ScheduleStatusResponse response = scheduler.getTasksStatus(
        new TaskQuery().setTaskIds(ImmutableSet.copyOf(taskIds)));

    if (response.getResponseCode() != ResponseCode.OK) {
      LOG.info("Got bad response code from scheduler, msg: " + response.getMessage());
      return null;
    }

    Map<String, TaskEvent> latestEvents = Maps.newHashMap();
    for (LiveTask task : response.getTasks()) {
      latestEvents.put(task.getScheduledTask().getAssignedTask().getTaskId(),
          Iterables.getLast(task.getScheduledTask().getTaskEvents()));
    }

    return latestEvents;
  }
}
