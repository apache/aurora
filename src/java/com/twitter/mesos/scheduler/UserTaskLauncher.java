package com.twitter.mesos.scheduler;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.scheduler.async.OfferQueue;
import com.twitter.mesos.scheduler.base.Conversions;
import com.twitter.mesos.scheduler.base.Query;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task launcher that matches resource offers against user tasks.
 */
class UserTaskLauncher implements TaskLauncher {

  private static final Logger LOG = Logger.getLogger(UserTaskLauncher.class.getName());

  @VisibleForTesting
  static final String MEMORY_LIMIT_EXCEEDED = "Memory limit exceeded";

  private final OfferQueue offerQueue;
  private final StateManager stateManager;

  @Inject
  UserTaskLauncher(OfferQueue offerQueue, StateManager stateManager) {
    this.offerQueue = checkNotNull(offerQueue);
    this.stateManager = checkNotNull(stateManager);
  }

  @Override
  public Optional<TaskInfo> createTask(Offer offer) {
    checkNotNull(offer);

    offerQueue.addOffer(offer);
    return Optional.absent();
  }

  @Override
  public synchronized boolean statusUpdate(TaskStatus status) {
    @Nullable String message = null;
    if (status.hasMessage()) {
      message = status.getMessage();
    }

    try {
      ScheduleStatus translatedState = Conversions.convertProtoState(status.getState());
      // TODO(William Farner): Remove this hack once MESOS-1793 is satisfied.
      if ((translatedState == ScheduleStatus.FAILED)
          && (message != null)
          && (message.contains(MEMORY_LIMIT_EXCEEDED))) {
        message = MEMORY_LIMIT_EXCEEDED;
      }

      stateManager.changeState(
          Query.byId(status.getTaskId().getValue()),
          translatedState,
          Optional.fromNullable(message));
    } catch (SchedulerException e) {
      LOG.log(Level.WARNING, "Failed to update status for: " + status, e);
      throw e;
    }
    return true;
  }

  @Override
  public void cancelOffer(OfferID offer) {
    offerQueue.cancelOffer(offer);
  }
}
