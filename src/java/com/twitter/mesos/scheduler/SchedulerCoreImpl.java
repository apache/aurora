package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.mesos.gen.UpdateResult;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveOffer;

import com.twitter.common.base.Closure;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.StateMachine;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ResourceConsumption;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.StateManager.StateChanger;
import com.twitter.mesos.scheduler.StateManager.StateMutation;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;
import static com.twitter.mesos.Tasks.SCHEDULED_TO_SHARD_ID;
import static com.twitter.mesos.Tasks.jobKey;
import static com.twitter.mesos.gen.ScheduleStatus.ASSIGNED;
import static com.twitter.mesos.gen.ScheduleStatus.KILLED_BY_CLIENT;
import static com.twitter.mesos.gen.ScheduleStatus.PENDING;
import static com.twitter.mesos.gen.ScheduleStatus.RESTARTING;
import static com.twitter.mesos.gen.ScheduleStatus.UNKNOWN;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.CONSTRUCTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.INITIALIZED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STARTED;
import static com.twitter.mesos.scheduler.SchedulerCoreImpl.State.STOPPED;

/**
 * Implementation of the scheduler core.
 *
 * @author William Farner
 */
public class SchedulerCoreImpl implements SchedulerCore {

  private static final Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  private final Map<String, VolatileTaskState> taskStateById =
      new MapMaker().makeComputingMap(new Function<String, VolatileTaskState>() {
        @Override public VolatileTaskState apply(String taskId) {
          return new VolatileTaskState();
        }
      });

  private final CronJobManager cronScheduler;

  // Schedulers that are responsible for triggering execution of jobs.
  private final ImmutableList<JobManager> jobManagers;

  // State manager handles persistence of task modifications and state transitions.
  private final StateManager stateManager;

  // Filter to determine whether a task should be scheduled.
  private final SchedulingFilter schedulingFilter;

  private final PulseMonitor<String> executorPulseMonitor;
  private final Function<TwitterTaskInfo, TwitterTaskInfo> executorResourceAugmenter;

  enum State {
    CONSTRUCTED,
    INITIALIZED,
    STARTED,
    STOPPED
  }

  private final StateMachine<State> stateMachine;

  @Inject
  public SchedulerCoreImpl(CronJobManager cronScheduler,
      ImmediateJobManager immediateScheduler,
      StateManager stateManager,
      SchedulingFilter schedulingFilter,
      PulseMonitor<String> executorPulseMonitor,
      Function<TwitterTaskInfo, TwitterTaskInfo> executorResourceAugmenter) {


    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    this.jobManagers = ImmutableList.of(cronScheduler, immediateScheduler);
    this.cronScheduler = cronScheduler;
    this.stateManager = checkNotNull(stateManager);

    this.schedulingFilter = checkNotNull(schedulingFilter);
    this.executorPulseMonitor = checkNotNull(executorPulseMonitor);
    this.executorResourceAugmenter = checkNotNull(executorResourceAugmenter);

   // TODO(John Sirois): Add a method to StateMachine or write a wrapper that allows for a
   // read-locked do-in-state assertion around a block of work.  Transition would then need to grab
   // the write lock.  Another approach is to force these transitions with:
   // SchedulerCoreFactory -> SchedulerCoreRunner -> SchedulerCore which remove all state sensitive
   // methods out of schedulerCore save for stop.
   stateMachine = StateMachine.<State>builder("scheduler-core")
       .initialState(CONSTRUCTED)
       .addState(CONSTRUCTED, INITIALIZED)
       .addState(INITIALIZED, STARTED)
       .addState(STARTED, STOPPED)
       .build();
  }

  @Override
  public String initialize() {
    checkLifecycleState(CONSTRUCTED);
    String storedFrameworkId = stateManager.initialize();
    stateMachine.transition(INITIALIZED);
    return storedFrameworkId;
  }

  @Override
  public void start(Closure<String> killTask) {
    checkLifecycleState(INITIALIZED);
    stateManager.start(killTask);
    stateMachine.transition(STARTED);

    for (JobManager jobManager : jobManagers) {
      jobManager.start();
    }
  }

  @Override
  public void registered(String frameworkId) {
    checkStarted();
    stateManager.setFrameworkId(frameworkId);
  }

  @Override
  public synchronized Set<TaskState> getTasks(final Query query) {
    checkStarted();

    Set<ScheduledTask> tasks = stateManager.fetchTasks(query);
    return ImmutableSet.copyOf(Iterables.transform(tasks,
        new Function<ScheduledTask, TaskState>() {
          @Override public TaskState apply(ScheduledTask task) {
            VolatileTaskState volatileTaskState = taskStateById.get(Tasks.id(task));
            return new TaskState(task, volatileTaskState);
          }
        }));
  }

  @Override
  public Iterable<TwitterTaskInfo> apply(Query query) {
    return Iterables.transform(getTasks(query), TaskState.STATE_TO_INFO);
  }

  private boolean hasActiveJob(JobConfiguration job) {
    return Iterables.any(jobManagers, managerHasJob(job));
  }

  private boolean hasRunningTasks(JobConfiguration job) {
    return !getTasks(Query.activeQuery(job)).isEmpty();
  }

  // TODO(William Farner): This is does not currently clear out tasks when a host is decommissioned.
  //    Figure out a solution that will work.  Might require mesos support for fetching the list
  //    of slaves.
  @Override
  public synchronized void updateRegisteredTasks(final RegisteredTaskUpdate update) {
    checkStarted();
    checkNotNull(update);
    checkNotBlank(update.getSlaveHost());
    checkNotNull(update.getTaskInfos());

    executorPulseMonitor.pulse(update.getSlaveHost());

    List<LiveTaskInfo> taskInfos = update.isSetTaskInfos() ? update.getTaskInfos()
        : ImmutableList.<LiveTaskInfo>of();

    // Wrap with a mutable map so we can modify later.
    final Map<String, LiveTaskInfo> taskInfoMap = Maps.newHashMap(
        Maps.uniqueIndex(taskInfos, Tasks.LIVE_TO_ID));

    // TODO(William Farner): Have the scheduler only retain configurations for live jobs,
    //    and acquire all other state from slaves.
    //    This will allow the scheduler to only persist active tasks.

    stateManager.taskOperation(new StateMutation.Quiet() {
      @Override public void execute(Set<ScheduledTask> tasks, StateChanger changer) {
        // Look for any tasks that we don't know about, or this slave should not be modifying.
        Set<String> tasksForHost = stateManager.fetchTaskIds(
            new Query(new TaskQuery().setSlaveHost(update.getSlaveHost())));

        Set<String> tasksForOtherHosts = stateManager.fetchTaskIds(
            new Query(new TaskQuery().setTaskIds(taskInfoMap.keySet()),
                new Predicate<ScheduledTask>() {
                  @Override public boolean apply(ScheduledTask task) {
                    return !update.getSlaveHost().equals(task.getAssignedTask().getSlaveHost());
                  }
                })
        );
        if (!tasksForOtherHosts.isEmpty()) {
          LOG.log(Level.SEVERE, "Slave " + update.getSlaveHost()
              + " sent an update for task(s) not assigned to it: " + update);
        }

        // We will only take action on tasks that we believe to be running on the host, or tasks
        // that were reported by the slave so long as they are not allocated to a different slave.
        Set<String> tasksToActOn =
            Sets.union(tasksForHost,
                Sets.difference(taskInfoMap.keySet(), tasksForOtherHosts));
        for (String taskId : tasksToActOn) {
          LiveTaskInfo taskUpdate = taskInfoMap.get(taskId);
          changer.changeState(
              ImmutableSet.of(taskId),
              taskUpdate == null ? UNKNOWN : taskUpdate.getStatus());

          // Update the resource information for the tasks that we currently have on record.
          if (taskUpdate != null && taskUpdate.getResources() != null) {
            taskStateById.get(taskId).resources =
                new ResourceConsumption(taskUpdate.getResources());
          }
        }
      }
    });
  }

  @Override
  public synchronized void createJob(JobConfiguration job) throws ScheduleException,
      ConfigurationManager.TaskDescriptionException {
    checkStarted();

    final JobConfiguration populated = ConfigurationManager.validateAndPopulate(job);

    if (hasActiveJob(populated)) {
      throw new ScheduleException("Job already exists: " + jobKey(populated));
    }

    boolean accepted = false;
    for (final JobManager manager : jobManagers) {
      if (manager.receiveJob(populated)) {
        LOG.info("Job accepted by manager: " + manager.getUniqueKey());
        accepted = true;
        break;
      }
    }

    if (!accepted) {
      LOG.severe("Job was not accepted by any of the configured schedulers, discarding.");
      LOG.severe("Discarded job: " + populated);
      throw new ScheduleException("Job not accepted, discarding.");
    }
  }

  @Override public void startCronJob(String role, String job) throws ScheduleException {
    checkNotBlank(role);
    checkNotBlank(job);
    checkStarted();

    String key = Tasks.jobKey(role, job);
    if (!cronScheduler.hasJob(key)) {
      throw new ScheduleException("Cron job does not exist for " + key);
    }

    cronScheduler.startJobNow(key);
  }

  @Override
  public synchronized void runJob(JobConfiguration job) {
    checkNotNull(job);
    checkNotNull(job.getTaskConfigs());
    checkStarted();
    checkState(!hasRunningTasks(job));

    launchTasks(job.getTaskConfigs());
  }

  /**
   * Launches tasks.
   *
   * @param tasks Tasks to launch.
   * @return The task IDs of the new tasks.
   */
  private Set<String> launchTasks(Set<TwitterTaskInfo> tasks) {
    if (tasks.isEmpty()) {
      return ImmutableSet.of();
    }

    LOG.info("Launching " + tasks.size() + " tasks.");
    return stateManager.insertTasks(tasks);
  }

  /**
   * Creates a predicate that will determine whether a job manager has a job matching a job key.
   *
   * @param job Job to match.
   * @return A new predicate matching the job owner and name given.
   */
  private static Predicate<JobManager> managerHasJob(final JobConfiguration job) {
    return new Predicate<JobManager>() {
      @Override public boolean apply(JobManager manager) {
        return manager.hasJob(jobKey(job));
      }
    };
  }

  private static TwitterTaskInfo makeBootstrapTask() {
    return new TwitterTaskInfo()
        .setOwner(new Identity("mesos", "mesos"))
        .setJobName("executor_bootstrap")
        .setNumCpus(1)
        .setRamMb(512)
        .setShardId(0)
        .setStartCommand("echo \"Bootstrapping\"");
  }

  @Override
  @Nullable
  public synchronized TwitterTask offer(final SlaveOffer slaveOffer) throws ScheduleException {
    checkStarted();
    checkNotNull(slaveOffer);

    vars.resourceOffers.incrementAndGet();

    final TwitterTaskInfo offer;
    try {
      offer = ConfigurationManager.makeConcrete(slaveOffer);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    final String hostname = slaveOffer.getHostname();
    Predicate<TwitterTaskInfo> hostResourcesFilter = schedulingFilter.staticFilter(offer, hostname);

    Query query;
    Predicate<TwitterTaskInfo> postFilter;
    if (!executorPulseMonitor.isAlive(hostname)) {
      TwitterTaskInfo bootstrapTask = makeBootstrapTask();

      // Mesos core does not account for the resources the executor itself will use up when it has
      // not yet started - do that accounting here and fail fast if we can't both run the executor
      // and the bootstrap task on the given host.
      if (!hostResourcesFilter.apply(executorResourceAugmenter.apply(bootstrapTask))) {
        LOG.severe(String.format(
            "Insufficient resources on host %s to start executor plus a bootstrap task", hostname));
        return null;
      }

      LOG.info("Pulse monitor considers executor dead, launching bootstrap task on: " + hostname);
      executorPulseMonitor.pulse(hostname);
      query = Query.byId(Iterables.getOnlyElement(launchTasks(ImmutableSet.of(bootstrapTask))));
      postFilter = Predicates.alwaysTrue();
      vars.executorBootstraps.incrementAndGet();
    } else {
      query = Query.and(
          Query.byStatus(PENDING),
          Predicates.compose(hostResourcesFilter, Tasks.SCHEDULED_TO_INFO));
      // Perform the (more expensive) check to find tasks matching the dynamic state of the machine.
      postFilter = schedulingFilter.dynamicHostFilter(this, hostname);
    }

    ImmutableSortedSet<ScheduledTask> candidates = ImmutableSortedSet.copyOf(
        Tasks.SCHEDULING_ORDER.onResultOf(Tasks.SCHEDULED_TO_ASSIGNED),
        stateManager.fetchTasks(query));
    if (candidates.isEmpty()) {
      return null;
    }

    ScheduledTask assignment = Iterables.get(Iterables.filter(
        candidates, Predicates.compose(postFilter, Tasks.SCHEDULED_TO_INFO)), 0, null);
    if (assignment == null) {
      return null;
    }

    AssignedTask task = stateManager.changeState(Query.byId(Tasks.id(assignment)), ASSIGNED,
        new Function<ScheduledTask, AssignedTask>() {
          @Override public AssignedTask apply(ScheduledTask task) {
            AssignedTask assigned = task.getAssignedTask();
            assigned.setSlaveHost(hostname);
            assigned.setSlaveId(slaveOffer.getSlaveId().getValue());
            return assigned;
          }
        });

    // There were no PENDING candidates.
    if (task == null) {
      return null;
    }

    LOG.info(String.format("Offer on slave %s (id %s) is being assigned task for %s.",
        hostname, task.getSlaveId(), jobKey(task)));

    List<Resource> resources = ImmutableList.of(
        Resources.makeResource(Resources.CPUS, task.getTask().getNumCpus()),
        Resources.makeResource(Resources.RAM_MB, task.getTask().getRamMb())
    );

    return makeTwitterTask(task, slaveOffer.getSlaveId().getValue(), resources);
  }

  @VisibleForTesting
  static TwitterTask makeTwitterTask(AssignedTask task, String slaveId, List<Resource> resources) {
    return new TwitterTask(task.getTaskId(), slaveId,
        task.getTask().getJobName() + "-" + task.getTaskId(), resources, task);
  }

  @Override
  public synchronized void setTaskStatus(Query rawQuery, final ScheduleStatus status,
      @Nullable final String message) {
    checkStarted();
    checkNotNull(rawQuery);
    checkNotNull(status);

    stateManager.changeState(rawQuery, status);
  }

  @Override
  public synchronized void killTasks(final Query query) throws ScheduleException {
    checkStarted();
    checkNotNull(query);

    LOG.info("Killing tasks matching " + query);

    // If this looks like a query for all tasks in a job, instruct the scheduler modules to
    // delete the job.
    boolean matchingScheduler = false;

    if (query.specifiesJobOnly()) {
      String jobKey = query.getJobKey();

      for (JobManager manager : jobManagers) {
        if (manager.deleteJob(jobKey)) {
          matchingScheduler = true;
        }
      }
    }

    if (!matchingScheduler) {
      stateManager.changeState(query, KILLED_BY_CLIENT, "Manually killed by client.");
    }
  }

  @Override
  public synchronized void restartTasks(final Set<String> taskIds) throws RestartException {
    checkStarted();
    checkNotBlank(taskIds);

    LOG.info("Restart requested for tasks " + taskIds);

    // TODO(William Farner): Change this (and the thrift interface) to query by shard ID in the
    //    context of a job instead of task ID.

    stateManager.taskOperation(Query.byId(taskIds),
        new StateMutation<RestartException>() {
          @Override public void execute(Set<ScheduledTask> tasks, StateChanger changer)
              throws RestartException {

            if (tasks.size() != taskIds.size()) {
              Set<String> unknownTasks = Sets.difference(taskIds, ImmutableSet
                  .copyOf(transform(tasks, Tasks.SCHEDULED_TO_ID)));

              throw new RestartException("Restart requested for unknown tasks " + unknownTasks);
            } else if (Iterables.any(tasks, Predicates.not(Tasks.ACTIVE_FILTER))) {
              throw new RestartException("Restart requested for inactive tasks "
                  + Iterables.filter(tasks, Tasks.ACTIVE_FILTER));
            }

            Set<String> jobKeys = ImmutableSet.copyOf(transform(tasks, Tasks.SCHEDULED_TO_JOB_KEY));
            if (jobKeys.size() != 1) {
              throw new RestartException(
                  "Task restart request cannot span multiple jobs: " + jobKeys);
            }

            changer.changeState(
                ImmutableSet.copyOf(Iterables.transform(tasks, Tasks.SCHEDULED_TO_ID)),
                RESTARTING, "Restarting by client request");
          }
        });
  }

  @Override
  public synchronized String startUpdate(JobConfiguration job)
      throws ScheduleException, ConfigurationManager.TaskDescriptionException {
    checkStarted();

    JobConfiguration populated = ConfigurationManager.validateAndPopulate(job);
    try {
      return stateManager.registerUpdate(Tasks.jobKey(job), populated.getTaskConfigs());
    } catch (StateManager.UpdateException e) {
      LOG.log(Level.INFO, "Failed to start update.", e);
      throw new ScheduleException(e);
    }
  }

  @Override
  public synchronized void updateShards(String role, String jobName, Set<Integer> shards,
      String updateToken) throws ScheduleException {
    checkStarted();

    String jobKey = Tasks.jobKey(role, jobName);
    Set<ScheduledTask> tasks =
       stateManager.fetchTasks(Query.liveShards(jobKey, shards));

    // Extract any shard IDs that are being added as a part of this stage in the update.
    Set<Integer> newShardIds = Sets.difference(shards,
        ImmutableSet.copyOf(Iterables.transform(tasks, SCHEDULED_TO_SHARD_ID)));

    if (!newShardIds.isEmpty()) {
      Set<TwitterTaskInfo> newTasks = stateManager.fetchUpdatedTaskConfigs(jobKey, newShardIds);
      Set<Integer> unrecognizedShards = Sets.difference(newShardIds,
          ImmutableSet.copyOf(Iterables.transform(newTasks, Tasks.INFO_TO_SHARD_ID)));
      if (!unrecognizedShards.isEmpty()) {
        throw new ScheduleException("Cannot update unrecognized shards " + unrecognizedShards);
      }

      // Create new tasks, so they will be moved into the PENDING state.
      stateManager.insertTasks(newTasks);
    }

    // Initiate update on the existing shards.
    stateManager.changeState(Query.liveShards(jobKey, Sets.difference(shards, newShardIds)),
        ScheduleStatus.UPDATING);
  }

  @Override
  public synchronized void rollbackShards(String role, String jobName, Set<Integer> shards,
      String updateToken) throws ScheduleException {
    checkStarted();

    stateManager.changeState(Query.liveShards(Tasks.jobKey(role, jobName), shards),
        ScheduleStatus.ROLLBACK);
  }

  @Override
  public synchronized void finishUpdate(String role, String jobName, @Nullable String updateToken,
      UpdateResult result) throws ScheduleException {
    checkStarted();

    String jobKey = Tasks.jobKey(role, jobName);
    try {
      stateManager.finishUpdate(jobKey, updateToken, result);
    } catch (StateManager.UpdateException e) {
      LOG.log(Level.INFO, "Failed to finish update.", e);
      throw new ScheduleException(e);
    }
  }

  @Override
  public synchronized void preemptTask(AssignedTask task, AssignedTask preemptingTask) {
    checkNotNull(task);
    checkNotNull(preemptingTask);
    // TODO(William Farner): Throw SchedulingException if either task doesn't exist, etc.

    stateManager.changeState(Query.byId(task.getTaskId()), ScheduleStatus.PREEMPTING,
        "Preempting in favor of " + Tasks.jobKey(preemptingTask));
  }

  @Override
  public void stop() {
    checkStarted();
    stateManager.stop();
    stateMachine.transition(STOPPED);
  }

  private void checkStarted() {
    checkLifecycleState(STARTED);
  }

  private void checkLifecycleState(State state) {
    Preconditions.checkState(stateMachine.getState() == state);
  }

  private final class Vars {
    final AtomicLong resourceOffers = Stats.exportLong("scheduler_resource_offers");
    final AtomicLong executorBootstraps = Stats.exportLong("executor_bootstraps");

    Vars() {
      for (final ScheduleStatus status : ScheduleStatus.values()) {
        Stats.export(new StatImpl<Integer>("task_store_" + status) {
          @Override public Integer read() {
            return stateManager.fetchTasks(Query.byStatus(status)).size();
          }
        });
      }
    }
  }
  private final Vars vars = new Vars();
}
