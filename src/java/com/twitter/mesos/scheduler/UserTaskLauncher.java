package com.twitter.mesos.scheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.async.TaskScheduler;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task launcher that matches resource offers against user tasks.
 */
class UserTaskLauncher implements TaskLauncher {

  private static final Logger LOG = Logger.getLogger(UserTaskLauncher.class.getName());

  @VisibleForTesting
  static final String MEMORY_LIMIT_EXCEEDED = "Memory limit exceeded";

  private final TaskScheduler taskScheduler;
  private final StateManager stateManager;

  @Inject
  UserTaskLauncher(TaskScheduler taskScheduler, StateManager stateManager) {
    this.taskScheduler = checkNotNull(taskScheduler);
    this.stateManager = checkNotNull(stateManager);
  }

  @Override
  public Optional<TaskInfo> createTask(Offer offer) {
    checkNotNull(offer);

    stateManager.saveAttributesFromOffer(offer.getHostname(), offer.getAttributesList());
    taskScheduler.offer(ImmutableList.of(offer));

    return Optional.absent();
  }

  @Override
  public synchronized boolean statusUpdate(TaskStatus status) {
    @Nullable String message = null;
    if (status.hasMessage()) {
      message = status.getMessage();
    }

    TaskQuery query = Query.byId(status.getTaskId().getValue());

    try {
      ScheduleStatus translatedState = StateTranslator.get(status.getState());
      if (translatedState == null) {
        LOG.severe("Failed to look up task state translation for: " + status.getState());
      } else {
        // TODO(William Farner): Remove this hack once MESOS-1793 is satisfied.
        if ((translatedState == ScheduleStatus.FAILED)
            && (message != null)
            && (message.contains(MEMORY_LIMIT_EXCEEDED))) {
          message = MEMORY_LIMIT_EXCEEDED;
        }

        stateManager.changeState(query, translatedState, Optional.fromNullable(message));
        return true;
      }
    } catch (SchedulerException e) {
      LOG.log(Level.WARNING, "Failed to update status for: " + status, e);
      throw e;
    }
    return false;
  }

  @Override
  public void cancelOffer(OfferID offer) {
    taskScheduler.cancelOffer(offer);
  }
}
