package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
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
import java.util.Deque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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

  // Stores work to perform using the scheduler driver.
  private Deque<Closure<SchedulerDriver>> workQueue = Lists.newLinkedList();

  @Inject
  public SchedulerCore(CronJobScheduler cronScheduler, ImmediateJobScheduler immediateScheduler) {
    // The immediate scheduler will accept any job, so it's important that other schedulers are
    // placed first.
    Closure<JobConfiguration> jobRunner = new Closure<JobConfiguration>() {
      @Override
      public void execute(JobConfiguration job) {
        LOG.info("Running job: " + job);
        runJob(job);
      }
    };
    jobSchedulers = Arrays.asList(cronScheduler.setJobRunner(jobRunner),
        immediateScheduler.setJobRunner(jobRunner));
  }

  /**
   * Assigns a framework ID to the scheduler, should be called when the scheduler implementation
   * has received a successful registration signal.
   *
   * @param frameworkId Framework ID.
   */
  public void setFrameworkId(int frameworkId) {
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
   * Checks whether the scheduler has any tasks matching the given query.
   *
   * @param owner The owner to look up.
   * @param jobName The job name to look up.
   * @return {@code true} if the scheduler has a job for the given owner and job name,
   *    {@code false} otherwise.
   */
  private boolean hasJob(final String owner, final String jobName) {
    TaskQuery query = new TaskQuery().setOwner(owner).setJobName(jobName);

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
   * Adds pending tasks, which will become candidates for scheduling the next time
   * {@link #offer(SlaveOffer)} is called.
   *
   * @param job The configuration of the job to create tasks for.
   * @throws ScheduleException If there was an error scheduling a cron job.
   */
  public synchronized void addTasks(JobConfiguration job) throws ScheduleException {
    Preconditions.checkNotNull(job);

    if (hasJob(job.getOwner(), job.getName())) {
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
      task.setStatus(status);
    }

    // TODO(wfarner): May want to add a fixed time delay on removing the task from tracking, to
    // allow it to remain in a failed/killed/lost state long enough for someone to debug.
    // This could be accomplished by using the work queue (possibly augmenting to use futures).
    TaskQuery completedQuery = new TaskQuery(query);
    completedQuery.addToStatuses(ScheduleStatus.FINISHED);
    completedQuery.addToStatuses(ScheduleStatus.KILLED);
    completedQuery.addToStatuses(ScheduleStatus.FAILED);
    completedQuery.addToStatuses(ScheduleStatus.LOST);
    completedQuery.addToStatuses(ScheduleStatus.NOT_FOUND);
    tasks.removeAll(Lists.newArrayList(getTasks(completedQuery)));
  }

  /**
   * Kills a specific set of tasks.
   *
   * @param query The query to identify tasks
   */
  public synchronized void killTasks(final TaskQuery query) {
    Preconditions.checkNotNull(query);

    // First remove all pending tasks matching the query.
    TaskQuery pendingQuery = new TaskQuery(query);
    pendingQuery.addToStatuses(ScheduleStatus.PENDING);
    tasks.removeAll(Lists.newArrayList(getTasks(pendingQuery)));

    // If this looks like a query for all tasks in a job, instruct the scheduler modules to delete
    // the job.
    if (!StringUtils.isEmpty(query.getOwner()) && !StringUtils.isEmpty(query.getJobName())
        && query.getTaskIdsSize() == 0) {
      for (JobScheduler scheduler : jobSchedulers) {
        scheduler.deleteJob(query.getOwner(), query.getJobName());
      }
    }

    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        LOG.info("Killing tasks matching " + query);

        for (TrackedTask task : getTasks(query)) {
          if (!task.isSetTaskId()) {
            // TODO(wfarner): These tasks should be removed from the tasks data structure.
            LOG.severe("Attempted to kill a task that does not yet have a task id");
          } else {
            driver.killTask(task.getTaskId());
          }
        }
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
    scheduleDriverWork(new Closure<SchedulerDriver>() {
      @Override public void execute(SchedulerDriver driver) throws RuntimeException {
        if (frameworkId.get() == -1) {
          LOG.info("Unable to restart tasks, framework not registered.");
          return;
        }

        LOG.info("Restarting tasks " + query);

        for (TrackedTask task : getTasks(query)) {
          if (task != null && task.status != ScheduleStatus.PENDING) {
            // TODO(wfarner): Once scheduler -> executorHub communication is defined, replace the
            // empty byte array with a serialized message.
            driver.sendFrameworkMessage(new FrameworkMessage(frameworkId.get(), task.slaveId,
                new byte[0]));
          }
        }
      }
    });
  }

  private void scheduleDriverWork(Closure<SchedulerDriver> work) {
    workQueue.addLast(Preconditions.checkNotNull(work));
  }

  public synchronized void clearWorkQueue(SchedulerDriver driver) {
    for (Closure<SchedulerDriver> work : workQueue) {
      work.execute(driver);
    }
    workQueue.clear();
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
