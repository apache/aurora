package com.twitter.mesos.scheduler.periodic;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.SlaveID;

import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.gen.comm.ExecutorMessage;
import com.twitter.mesos.scheduler.Driver;
import com.twitter.mesos.scheduler.MesosSchedulerImpl.SlaveHosts;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.StateManager;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Runner that collects inactive tasks for pruning, invokes the pruner, and communicates pruning
 * decisions to other parts of the system.
 *
 * @author William Farner
 */
class HistoryPruneRunner implements Runnable {

  private static final Logger LOG = Logger.getLogger(HistoryPruneRunner.class.getName());

  private final Driver driver;
  private final StateManager stateManager;
  private final HistoryPruner historyPruner;
  private final ExecutorID defaultExecutorId;
  private final SlaveHosts slaveHosts;

  @Inject
  HistoryPruneRunner(Driver driver,
      StateManager stateManager,
      HistoryPruner historyPruner,
      ExecutorID defaultExecutorId,
      SlaveHosts slaveHosts) {

    this.driver = checkNotNull(driver);
    this.stateManager = checkNotNull(stateManager);
    this.historyPruner = checkNotNull(historyPruner);
    this.defaultExecutorId = checkNotNull(defaultExecutorId);
    this.slaveHosts = checkNotNull(slaveHosts);
  }

  private static final Query INACTIVE_QUERY =
      new Query(new TaskQuery().setStatuses(EnumSet.complementOf(Tasks.ACTIVE_STATES)));

  private static final Function<ScheduledTask, String> TASK_TO_HOST =
      new Function<ScheduledTask, String>() {
        @Override public String apply(ScheduledTask task) {
          return task.assignedTask.getSlaveHost();
        }
      };

  private Query hostQuery(String host) {
    return new Query(new TaskQuery().setSlaveHost(host));
  }

  private Set<String> taskIds(Iterable<ScheduledTask> tasks) {
    return ImmutableSet.copyOf(Iterables.transform(tasks, Tasks.SCHEDULED_TO_ID));
  }

  @Timed("history_prune_runner")
  @Override
  public void run() {
    Set<ScheduledTask> inactiveTasks = stateManager.fetchTasks(INACTIVE_QUERY);
    if (inactiveTasks.isEmpty()) {
      LOG.info("No inactive tasks found.");
      return;
    }

    LOG.info("Fetched " + inactiveTasks.size() + " pruning candidates.");

    Set<ScheduledTask> pruneTasks = historyPruner.apply(inactiveTasks);
    if (pruneTasks.isEmpty()) {
      LOG.fine("No tasks found for pruning");
      return;
    }

    Set<String> pruneTaskIds =
        ImmutableSet.copyOf(Iterables.transform(pruneTasks, Tasks.SCHEDULED_TO_ID));
    LOG.info("Pruning " + pruneTasks.size() + " tasks: " + pruneTaskIds);

    stateManager.deleteTasks(pruneTaskIds);

    // Group pruned tasks by slave host.
    Multimap<String, ScheduledTask> pruneByHosts = Multimaps.index(pruneTasks, TASK_TO_HOST);
    Map<String, SlaveID> knownHosts = slaveHosts.getSlaves();

    // Filter and log any tasks associated with currently-unknown hosts.
    Set<String> unknownHosts = Sets.difference(pruneByHosts.keySet(), knownHosts.keySet());
    Predicate<String> unknownFilter = Predicates.in(unknownHosts);
    if (!unknownHosts.isEmpty()) {
      LOG.warning("No known slave IDs for hosts, unable to notify of deleted tasks: "
          + Multimaps.filterKeys(pruneByHosts, unknownFilter));
    }

    for (Map.Entry<String, Collection<ScheduledTask>> pruneByHost
        : Multimaps.filterKeys(pruneByHosts, Predicates.not(unknownFilter)).asMap().entrySet()) {

      String host = pruneByHost.getKey();
      Map<String, ScheduledTask> tasksOnHost =
          Maps.uniqueIndex(stateManager.fetchTasks(hostQuery(host)), Tasks.SCHEDULED_TO_ID);

      Predicate<String> retain = Predicates.not(Predicates.in(taskIds(pruneByHost.getValue())));
      Map<String, ScheduledTask> retainedTasks = Maps.filterKeys(tasksOnHost, retain);
      Map<String, ScheduleStatus> retainedWithStatus =
          Maps.transformValues(retainedTasks, Tasks.GET_STATUS);

      LOG.info("Instructing executor " + host + " to retain only tasks " + retainedWithStatus);

      AdjustRetainedTasks message = new AdjustRetainedTasks().setRetainedTasks(retainedWithStatus);
      driver.sendMessage(
          ExecutorMessage.adjustRetainedTasks(message),
          knownHosts.get(host),
          defaultExecutorId);
    }
  }
}
