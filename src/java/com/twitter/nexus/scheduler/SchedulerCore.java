package com.twitter.nexus.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.nexus.gen.JobConfiguration;
import com.twitter.nexus.gen.TaskQuery;
import com.twitter.nexus.gen.TwitterTaskInfo;
import com.twitter.nexus.gen.ScheduleStatus;
import com.twitter.nexus.gen.TrackedTask;
import nexus.FrameworkMessage;
import nexus.SchedulerDriver;
import nexus.SlaveOffer;
import nexus.StringMap;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Scheduling core, stores scheduler state and makes decisions about which tasks to schedule when
 * a resource offer is made.
 *
 * When a job is submitted to the scheduler core, it will store the job configuration and offer
 * the job to all configured scheduler modules, which are responsible for triggering execution of
 * the job.  Until a job is triggered by a scheduler module, it is retained in the scheduler core
 * in the PENDING state.
 *
 * TODO(wfarner): Figure out how to handle permissions for modification of running tasks.  As it
 * stands it would be quite easy to accidentally modify something that does not belong to you.
 *
 * @author wfarner
 */
public class SchedulerCore {
  private static Logger LOG = Logger.getLogger(SchedulerCore.class.getName());

  // The authoritative store for tasks that have been submited to the scheduler core.
  private final List<TrackedTask> tasks = Lists.newArrayList();

  // Schedulers that are responsible for triggering execution of jobs.
  private final List<JobScheduler> jobSchedulers;

  // TODO(wfarner): Hopefully we can abolish (or at least mask) the concept of canonical task IDs
  // in favor of tasks being canonically named by job/taskIndex.
  private int nextTaskId = 0;

  // The nexus framework ID of the scheduler, set to -1 until the framework is registered.
  private final AtomicInteger frameworkId = new AtomicInteger(-1);

  // Work queue that stores pending asynchronous tasks.
  @Inject
  private WorkQueue workQueue;

  @Inject
  private ZooKeeperClient zkClient;

  // Amount of time to wait before discarding record of a lost/failed task.
  private static final int TASK_REMOVE_DELAY_MINS = 1;

  // Scheduler driver used for communication with other nodes in the cluster.
  private final AtomicReference<SchedulerDriver> schedulerDriver =
      new AtomicReference<SchedulerDriver>();

  @Inject
  public SchedulerCore(CronJobScheduler cronScheduler, ImmediateJobScheduler immediateScheduler) {
    Closure<JobConfiguration> jobRunner = new Closure<JobConfiguration>() {
      @Override
      public void execute(JobConfiguration job) {
        LOG.info("Running job: " + job);
        runJob(job);
      }
    };

    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    jobSchedulers = Arrays.asList(cronScheduler.setJobRunner(jobRunner),
        immediateScheduler.setJobRunner(jobRunner));
  }

  /**
   * Assigns a framework ID to the scheduler, should be called when the scheduler implementation
   * has received a successful registration signal.
   *
   * @param driver The registered driver reference.
   * @param frameworkId Framework ID.
   */
  public void registered(SchedulerDriver driver, int frameworkId) {
    this.schedulerDriver.set(Preconditions.checkNotNull(driver));
    this.frameworkId.set(frameworkId);
  }

  /**
   * Fetches information about all registered tasks for a job.
   *
   * @param query The query to identify tasks.
   * @return An iterable of task objects.
   */
  public synchronized Iterable<TrackedTask> getTasks(final TaskQuery query) {
    Preconditions.checkNotNull(query);
    return Iterables.filter(tasks, taskMatcher(query));
  }

  /**
   * Checks whether the scheduler has a job matching the owner/jobName.
   *
   * @param owner The owner to look up.
   * @param jobName The job name to look up.
   * @return {@code true} if the scheduler has a job for the given owner and job name,
   *    {@code false} otherwise.
   */
  @VisibleForTesting
  synchronized boolean hasActiveJob(final String owner, final String jobName) {
    TaskQuery query = new TaskQuery().setOwner(owner).setJobName(jobName);
    for (ScheduleStatus status : ScheduleStatus.values()) {
      if (status != ScheduleStatus.KILLED) query.addToStatuses(status);
    }

    return !Iterables.isEmpty(getTasks(query)) || Iterables.any(jobSchedulers,
        new Predicate<JobScheduler>() {
          @Override public boolean apply(JobScheduler scheduler) {
            return scheduler.hasJob(owner, jobName);
          }
    });
  }

  private int generateTaskId() {
    return nextTaskId++;
  }

  /**
   * Creates a new job, whose tasks will become candidates for scheduling.
   *
   * @param job The configuration of the job to create tasks for.
   * @throws ScheduleException If there was an error scheduling a cron job.
   */
  public synchronized void createJob(JobConfiguration job) throws ScheduleException {
    Preconditions.checkNotNull(job);

    if (hasActiveJob(job.getOwner(), job.getName())) {
      throw new ScheduleException(String.format("Job already exists: %s/%s",
          job.getOwner(), job.getName()));
    }

    boolean accepted = false;
    for (JobScheduler scheduler : jobSchedulers) {
      if (scheduler.receiveJob(job)) {
        accepted = true;
        LOG.info("Job accepted by scheduler: " + scheduler.getClass().getName());
        break;
      }
    }

    if (!accepted) {
      LOG.severe("Job was not accepted by any of the configured schedulers, discarding.");
      LOG.severe("Discarded job: " + job);
      throw new ScheduleException("Job not accepted, discarding.");
    }
  }

  private synchronized void runJob(JobConfiguration job) {
    for (TwitterTaskInfo task : Preconditions.checkNotNull(job.getTaskConfigs())) {
      int taskId = generateTaskId();
      tasks.add(new TrackedTask()
          .setTaskId(taskId)
          .setJobName(job.getName())
          .setOwner(job.getOwner())
          .setTask(task)
          .setStatus(ScheduleStatus.PENDING));
    }
  }

  /**
   * Offers resources to the scheduler.  If the scheduler has a pending task that is satisfied by
   * the offer, it will return the task description.
   *
   * @param slaveOffer The slave offer.
   * @return A task description that defines the task to run, or {@code null} if there are no
   *    pending tasks that are satisfied by the slave offer.
   */
  public synchronized nexus.TaskDescription offer(SlaveOffer slaveOffer) {
    TwitterTaskInfo offer;
    try {
      offer = ConfigurationManager.makeConcrete(slaveOffer);
    } catch (ConfigurationManager.TaskDescriptionException e) {
      LOG.log(Level.SEVERE, "Invalid slave offer", e);
      return null;
    }

    TaskQuery query = new TaskQuery();
    query.addToStatuses(ScheduleStatus.PENDING);

    for (TrackedTask task : getTasks(query)) {
      TwitterTaskInfo taskInfo = task.getTask();
      if (ConfigurationManager.satisfied(taskInfo, offer)) {
        // TODO(wfarner): Remove this hack once nexus core does not read parameters.
        StringMap params = new StringMap();
        params.set("cpus", String.valueOf((int) taskInfo.getNumCpus()));
        params.set("mem", String.valueOf(taskInfo.getRamBytes()));

        task.status = ScheduleStatus.STARTING;
        task.slaveId = slaveOffer.getSlaveId();
        byte[] taskInBytes = null;
        try {
          taskInBytes = new TSerializer().serialize(taskInfo);
        } catch (TException e) {
           LOG.log(Level.SEVERE,"Error serializing Thrift TwitterTaskInfo",e);
          //todo(flo):maybe cleanup and exit cleanly
          throw new RuntimeException(e);
        }

        LOG.info(String.format("Offer on slave %d is being assigned task for %s/%s.",
            task.slaveId, task.getOwner(), task.getJobName()));

        return new nexus.TaskDescription(task.getTaskId(), slaveOffer.getSlaveId(),
                task.jobName + "-" + task.getTaskId(), params, taskInBytes);
      }
    }

    return null;
  }

  /**
   * Assigns a new state to tasks.
   *
   * TODO(wfarner): May want to specify contract on whether this can modify multiple tasks or must
   * match only one.
   *
   * @param query The query to identify tasks
   * @param status The new state of the tasks.
   */
  public synchronized void setTaskStatus(TaskQuery query, ScheduleStatus status) {
    Preconditions.checkNotNull(status);
    for (TrackedTask task : getTasks(query)) {
      if (task.getStatus() != ScheduleStatus.KILLED) task.setStatus(status);
    }

    // TODO(wfarner): Should add a fixed time delay on removing the task from tracking, to
    // allow it to remain in a failed/killed/lost state long enough for someone to debug.
    // This could be accomplished by using the work queue (possibly augmenting to use futures).

    // Fetch all completed tasks.
    TaskQuery completedQuery = new TaskQuery(query);
    completedQuery.setStatuses(Sets.newHashSet(
        ScheduleStatus.FINISHED,
        ScheduleStatus.KILLED,
        ScheduleStatus.FAILED,
        ScheduleStatus.LOST,
        ScheduleStatus.NOT_FOUND));
    Iterable<TrackedTask> completedTasks = getTasks(completedQuery);

    // TODO(wfarner): Add task failure tracking to TaskQuery and allow the configuration to
    // specify the number of per-task failures before giving up.
    Predicate<TrackedTask> daemonTaskFinder = new Predicate<TrackedTask>() {
      @Override public boolean apply(TrackedTask task) {
        return task.getTask().isIsDaemon();
      }
    };

    // Non-daemon tasks are finished and may be removed.
    for (TrackedTask task : Iterables.filter(completedTasks, Predicates.not(daemonTaskFinder))) {
      // TODO(wfarner): May want to consider allowing the executor to handle this by retaining
      // tasks for a period (forcibly removing them if the disk they consume is needed).
      removeWithDelay(task);
    }

    // Daemon tasks should be rescheduled.
    for (TrackedTask task : Iterables.filter(completedTasks, daemonTaskFinder)) {
      LOG.info("Moving daemon task to PENDING state: "
               + task.getOwner() + "/" + task.getJobName());
      task.setStatus(ScheduleStatus.PENDING);
      task.setSlaveIdIsSet(false);
      task.setSlaveId(-1);
    }
  }

  /**
   * Removes a task from tracking in the scheduler, after a fixed delay.
   *
   * @param task Task that are to be removed.
   */
  private void removeWithDelay(final TrackedTask task) {
    LOG.info("Will be removing task: " + task);
    workQueue.scheduleWork(new Callable<Boolean>() {
      @Override public Boolean call() {
        LOG.info("Removing completed task: " + task);
        tasks.removeAll(Arrays.asList(task));
        return true;
      }
    }, TASK_REMOVE_DELAY_MINS, TimeUnit.MINUTES);
  }

  /**
   * Kills a specific set of tasks.
   *
   * @param query The query to identify tasks
   */
  public synchronized void killTasks(final TaskQuery query) {
    Preconditions.checkNotNull(query);
    LOG.info("Killing tasks matching " + query);

    // If this looks like a query for all tasks in a job, instruct the scheduler modules to delete
    // the job.
    if (!StringUtils.isEmpty(query.getOwner()) && !StringUtils.isEmpty(query.getJobName())
        && query.getTaskIdsSize() == 0) {
      for (JobScheduler scheduler : jobSchedulers) {
        scheduler.deleteJob(query.getOwner(), query.getJobName());
      }
    }

    for (final TrackedTask task : getTasks(query)) {
      if (task.getStatus() == ScheduleStatus.PENDING) {
        removeWithDelay(task.setStatus(ScheduleStatus.KILLED));
      } else if (task.getStatus() != ScheduleStatus.KILLED) {
        doWorkWithDriver(new Function<SchedulerDriver, Integer>() {
          @Override public Integer apply(SchedulerDriver driver) {
            return driver.killTask(task.getTaskId());
          }
        });
      }
    }
  }

  /**
   * Executes a unit of work that uses the scheduler driver.  This exists as a convenience function
   * for any tasks that require use of the {@link SchedulerDriver}, for automatic retrying in the
   * event that the driver is not available.
   *
   * @param work The work to execute.  Should return the status code provided by the driver
   *    (0 denotes success, non-zero denotes a failure that should be retried).
   */
  private void doWorkWithDriver(final Function<SchedulerDriver, Integer> work) {
    workQueue.doWork(new Callable<Boolean>() {
      @Override public Boolean call() {
        if (frameworkId.get() == -1) {
          LOG.info("Unable to send framework messages, framework not registered.");
          return false;
        }

        // TODO(wfarner): What happens when this fails?  Do we have to reconnect manually, or does
        // the driver automatically try to reconnect when it sends a non-zero status code?
        SchedulerDriver driver = schedulerDriver.get();
        return driver != null && work.apply(driver) == 0;
      }
    });
  }

  /**
   * Schedules a restart on a set of tasks.
   *
   * @param query The query to identify tasks.
   */
  public synchronized void restartTasks(final TaskQuery query) {
    Preconditions.checkNotNull(query);

    // TODO(wfarner): Need to do this in a cleaner way so that the entire job doesn't flip at once.
    LOG.info("Restarting tasks " + query);

    for (final TrackedTask task : getTasks(query)) {
      if (task != null && task.status != ScheduleStatus.PENDING) {
        // TODO(wfarner): Once scheduler -> executorHub communication is defined, replace the
        // empty byte array with a serialized message.
        doWorkWithDriver(new Function<SchedulerDriver, Integer>() {
          @Override public Integer apply(SchedulerDriver driver) {
            return driver.sendFrameworkMessage(
                new FrameworkMessage(frameworkId.get(), task.slaveId, new byte[0]));
          }
        });
      }
    }
  }

  private void pushToZooKeeper() {

  }

  /**
   * Returns a predicate that will match tasks against the given {@code query}.
   *
   * @param query The query to use for finding tasks.
   * @return An iterable containing all matching tasks
   */
  private static Predicate<TrackedTask> taskMatcher(final TaskQuery query) {
    Preconditions.checkNotNull(query);
    return new Predicate<TrackedTask>() {
      private boolean matches(String query, String value) {
        return StringUtils.isEmpty(query) || value.matches(query);
      }

      private <T> boolean matches(Collection<T> collection, T item) {
        return collection == null || collection.isEmpty() || collection.contains(item);
      }

      @Override public boolean apply(TrackedTask task) {
        return matches(query.getOwner(), task.getOwner())
            && matches(query.getJobName(), task.getJobName())
            && matches(query.getTaskIds(), task.getTaskId())
            && matches(query.getStatuses(), task.getStatus());
      }
    };
  }
}
