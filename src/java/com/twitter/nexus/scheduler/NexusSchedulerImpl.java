package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.twitter.nexus.gen.ScheduleStatus;
import nexus.ExecutorInfo;
import nexus.Scheduler;
import nexus.SchedulerDriver;
import nexus.SlaveOfferVector;
import nexus.StringMap;
import nexus.TaskDescriptionVector;
import nexus.TaskStatus;

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
    schedulerCore.setFrameworkId(frameworkId);
  }

  @Override
  public void resourceOffer(SchedulerDriver driver, long offerId, SlaveOfferVector offers) {
    schedulerCore.clearWorkQueue(driver);

    TaskDescriptionVector newlyScheduledTasks = new TaskDescriptionVector();

    for (int i = 0; i < offers.size(); i++) {
      nexus.TaskDescription taskToSchedule = schedulerCore.schedulePendingTask(offers.get(i));
      if (taskToSchedule != null) {
        newlyScheduledTasks.add(taskToSchedule);
      }

      LOG.info("Accepting tasks: " + newlyScheduledTasks.size());
    }
    driver.replyToOffer(offerId, newlyScheduledTasks, new StringMap());
  }

  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    LOG.info("Received status update for task " + status.getTaskId()
        + " in state " + status.getState());

    if (schedulerCore.getTask(status.getTaskId()) == null) {
      LOG.severe("Failed to find task id " + status.getTaskId());
    } else {
      boolean removeTask = false;

      switch (status.getState()) {
        case TASK_STARTING:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.STARTING);
          break;
        case TASK_RUNNING:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.RUNNING);
          break;
        case TASK_FINISHED:
          // TODO(wfarner): Some of these states will require the task to be rescheduled,
          // depending on the config (daemon, number of allowed failures, etc).
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.FINISHED);
        case TASK_FAILED:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.FAILED);
        case TASK_KILLED:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.KILLED);
        case TASK_LOST:
          schedulerCore.setTaskStatus(status.getTaskId(), ScheduleStatus.LOST);
          removeTask = true;
          break;
        default:
          LOG.severe("Unrecognized task state " + status.getState());
          return;
      }

      // TODO(wfarner): May want to add a fixed time delay on removing the task from tracking, to
      // allow it to remain in a failed/killed/lost state long enough for someone to debug.
      if (removeTask) {
        schedulerCore.removeTask(status.getTaskId());
      }
    }
  }

  @Override
  public void error(SchedulerDriver driver, int code, String message) {
    LOG.severe("Received error message: " + message + " with code " + code);
  }
}
