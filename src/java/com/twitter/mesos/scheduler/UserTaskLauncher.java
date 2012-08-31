package com.twitter.mesos.scheduler;

import java.util.Set;
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
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.Tasks.jobKey;

/**
 * A task launcher that matches resource offers against user tasks.
 */
class UserTaskLauncher implements TaskLauncher {

  private static final Logger LOG = Logger.getLogger(UserTaskLauncher.class.getName());

  private final StateManager stateManager;
  private final MesosTaskFactory taskFactory;
  private final SchedulingFilter schedulingFilter;

  @Inject
  UserTaskLauncher(
      StateManager stateManager,
      MesosTaskFactory taskFactory,
      SchedulingFilter schedulingFilter) {

    this.stateManager = checkNotNull(stateManager);
    this.taskFactory = checkNotNull(taskFactory);
    this.schedulingFilter = checkNotNull(schedulingFilter);
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
      Set<Veto> vetoes = schedulingFilter.filter(
          Resources.from(offer),
          offer.getHostname(),
          task.getAssignedTask().getTask(),
          Tasks.id(task));
      if (vetoes.isEmpty()) {
        return Optional.of(assignTask(offer, task));
      } else {
        LOG.fine("Slave " + offer.getHostname() + " vetoed task " + Tasks.id(task) + ": " + vetoes);
      }
    }

    return Optional.absent();
  }

  private TaskInfo assignTask(Offer offer, ScheduledTask task) {
    String host = offer.getHostname();
    Set<Integer> selectedPorts =
        Resources.getPorts(offer, task.getAssignedTask().getTask().getRequestedPortsSize());
    task.setAssignedTask(
        stateManager.assignTask(Tasks.id(task), host, offer.getSlaveId(), selectedPorts));
    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
        host, offer.getSlaveId(), jobKey(task)));
    return taskFactory.createFrom(task.getAssignedTask(), offer.getSlaveId());
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
