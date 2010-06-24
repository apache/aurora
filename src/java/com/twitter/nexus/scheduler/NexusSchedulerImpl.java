package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.TaskQuery;
import nexus.ExecutorInfo;
import nexus.Scheduler;
import nexus.SchedulerDriver;
import nexus.SlaveOfferVector;
import nexus.StringMap;
import nexus.TaskDescriptionVector;
import nexus.TaskState;
import nexus.TaskStatus;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Location for communication with the nexus core.
 *
 * @author wfarner
 */
class NexusSchedulerImpl extends Scheduler {
  private static Logger LOG = Logger.getLogger(NexusSchedulerImpl.class.getName());

  static {
    System.loadLibrary("nexus");
  }

  // Stores scheduler state and handles actual scheduling decisions.
  private final SchedulerMain.TwitterSchedulerOptions options;
  private final SchedulerCore schedulerCore;

  @Inject
  public NexusSchedulerImpl(SchedulerMain.TwitterSchedulerOptions options,
      SchedulerCore schedulerCore) {
    this.options = Preconditions.checkNotNull(options);
    this.schedulerCore = Preconditions.checkNotNull(schedulerCore);
  }

  @Override
  public String getFrameworkName(SchedulerDriver driver) {
    return "TwitterScheduler";
  }

  @Override
  public ExecutorInfo getExecutorInfo(SchedulerDriver driver) {
    return new ExecutorInfo(options.executorPath, new byte[0]);
  }

  @Override
  public void registered(SchedulerDriver driver, int frameworkId) {
    LOG.info("Registered with ID " + frameworkId);
    // TODO(wfarner): Register with ZooKeeper
    schedulerCore.registered(driver, frameworkId);
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, long offerId, SlaveOfferVector offers) {
    TaskDescriptionVector newlyScheduledTasks = new TaskDescriptionVector();

    try {
      for (int i = 0; i < offers.size(); i++) {
        nexus.TaskDescription taskToSchedule = schedulerCore.offer(offers.get(i));
        if (taskToSchedule != null) {
          newlyScheduledTasks.add(taskToSchedule);
        }
      }
    } catch (ScheduleException e) {
      LOG.log(Level.SEVERE, "Failed to schedule offer.", e);
      return;
    }

    driver.replyToOffer(offerId, newlyScheduledTasks, new StringMap());
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    LOG.info("Received status update for task " + status.getTaskId()
        + " in state " + status.getState());

    TaskQuery query = new TaskQuery();
    query.addToTaskIds(status.getTaskId());

    if (Iterables.isEmpty(schedulerCore.getTasks(query))) {
      LOG.severe("Failed to find task id " + status.getTaskId());
    } else {
      ScheduleStatus translatedState = STATE_TRANSLATION.get(status.getState());
      if (translatedState == null) {
        LOG.log(Level.SEVERE, "Failed to look up task state translation for: " + status.getState());
        return;
      }

      schedulerCore.setTaskStatus(query, translatedState);
    }
  }

  @Override
  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
  }

  // Maps from nexus state to scheduler interface state.
  private static final Map<TaskState, ScheduleStatus> STATE_TRANSLATION =
      new ImmutableMap.Builder<TaskState, ScheduleStatus>()
        .put(TaskState.TASK_STARTING, ScheduleStatus.STARTING)
        .put(TaskState.TASK_RUNNING, ScheduleStatus.RUNNING)
        .put(TaskState.TASK_FINISHED, ScheduleStatus.FINISHED)
        .put(TaskState.TASK_FAILED, ScheduleStatus.FAILED)
        .put(TaskState.TASK_KILLED, ScheduleStatus.KILLED)
        .put(TaskState.TASK_LOST, ScheduleStatus.LOST)
        .build();
}
