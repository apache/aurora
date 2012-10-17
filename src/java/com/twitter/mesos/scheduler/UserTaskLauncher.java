package com.twitter.mesos.scheduler;

import java.util.SortedSet;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.mesos.StateTranslator;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task launcher that matches resource offers against user tasks.
 */
class UserTaskLauncher implements TaskLauncher {

  private static final Logger LOG = Logger.getLogger(UserTaskLauncher.class.getName());

  private final StateManager stateManager;
  private final TaskAssigner assigner;

  @Inject
  UserTaskLauncher(StateManager stateManager, TaskAssigner assigner) {
    this.stateManager = checkNotNull(stateManager);
    this.assigner = checkNotNull(assigner);
  }

  @Override
  public Optional<TaskInfo> createTask(Offer offer) {
    checkNotNull(offer);

    stateManager.saveAttributesFromOffer(offer.getHostname(), offer.getAttributesList());
    Iterable<ScheduledTask> schedulable =
        stateManager.fetchTasks(Query.byStatus(ScheduleStatus.PENDING));

    if (Iterables.isEmpty(schedulable)) {
      return Optional.absent();
    }

    SortedSet<ScheduledTask> candidates = ImmutableSortedSet.copyOf(
        Tasks.SCHEDULING_ORDER.onResultOf(Tasks.SCHEDULED_TO_ASSIGNED),
        schedulable);

    LOG.fine("Candidates for offer: " + Tasks.ids(candidates));

    for (ScheduledTask task : candidates) {
      Optional<TaskInfo> assignment = assigner.maybeAssign(offer, task);
      if (assignment.isPresent()) {
        return assignment;
      }
    }

    return Optional.absent();
  }

  @Override
  public synchronized boolean statusUpdate(TaskStatus status) {
    String info = status.hasData() ? status.getData().toStringUtf8() : null;
    TaskQuery query = Query.byId(status.getTaskId().getValue());

    try {
      if (stateManager.fetchTasks(query).isEmpty()) {
        LOG.severe("Failed to find task id " + status.getTaskId());
      } else {
        ScheduleStatus translatedState = StateTranslator.get(status.getState());
        if (translatedState == null) {
          LOG.severe("Failed to look up task state translation for: " + status.getState());
        } else {
          stateManager.changeState(query, translatedState, Optional.fromNullable(info));
          return true;
        }
      }
    } catch (SchedulerException e) {
      LOG.log(Level.WARNING, "Failed to update status for: " + status, e);
      throw e;
    }
    return false;
  }
}
