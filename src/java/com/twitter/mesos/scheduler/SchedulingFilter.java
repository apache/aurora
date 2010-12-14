package com.twitter.mesos.scheduler;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 *
 * @author wfarner
 */
public interface SchedulingFilter {

  /**
   * Creates a new filter that will make decisions about whether tasks meet a resource offer and
   * may be assigned to a host.
   *
   * @param resourceOffer The resource offer to check against tasks.
   * @param slaveHost The slave host that the resource offer is associated with.
   * @return A new predicate that can be used to find tasks meeting the offer.
   */
  public Predicate<TaskState> makeFilter(TwitterTaskInfo resourceOffer, String slaveHost);

  /**
   * Implementation of the scheduling filter that ensures resource requirements of tasks are
   * fulfilled, and that tasks are allowed to run on the given machine.
   */
  public static class SchedulingFilterImpl implements SchedulingFilter {

    /**
     * {@literal @Named} binding key for the machine reservation map..
     */
    public static final String MACHINE_RESTRICTIONS =
        "com.twitter.mesos.scheduler.MACHINE_RESTRICTIONS";

    private final SchedulerCore scheduler;
    private final Map<String, String> machineRestrictions;

    /**
     * Creates a new scheduling filter.
     *
     * @param scheduler Scheduler to look up existing task information.
     * @param machineRestrictions Mapping from machine host name to job key.  Restricted machines
     *    may only run the specified job key, and the specified job key may only run on the
     *    respective machine.
     */
    @Inject
    public SchedulingFilterImpl(SchedulerCore scheduler,
        @Named(MACHINE_RESTRICTIONS) Map<String, String> machineRestrictions) {
      this.scheduler = checkNotNull(scheduler);
      this.machineRestrictions = checkNotNull(machineRestrictions);
    }

    /**
     * Tests whether a machine is allowed to run a job.  A job may only run on a machine if the
     * machine is designated for the job, or the machine is not restricted.
     *
     * @param slaveHost The host to check against.
     * @param jobKey The job to test.
     * @return {@code true} if the machine may run the job, {@code false} otherwise.
     */
    private boolean machineCanRunJob(String slaveHost, String jobKey) {
      String machineRestrictedToJob = machineRestrictions.get(slaveHost);

      return machineRestrictedToJob == null || machineRestrictedToJob.equals(jobKey);
    }

    /**
     * Tests whether a job is allowed to run on a machine.  A job may run on a machine if the
     * machine is reserved for the job, or the job has no machine reservations.
     *
     * @param slaveHost The host to test.
     * @param jobKey The job to check against.
     * @return {@code true} if the job may run on the machine, {@code false} otherwise.
     */
    private boolean jobCanRunOnMachine(String slaveHost, String jobKey) {
      boolean foundJobRestriction = false;
      for (Map.Entry<String, String> hostToJob : machineRestrictions.entrySet()) {
        if (hostToJob.getValue().equals(jobKey)) {
          foundJobRestriction = true;
          if (hostToJob.getKey().equals(slaveHost)) return true;
        }
      }

      return !foundJobRestriction;
    }

    private Predicate<TaskState> meetsMachineReservation(final String slaveHost) {
      return new Predicate<TaskState>() {
        @Override public boolean apply(TaskState state) {
          String jobKey = Tasks.jobKey(state);
          return machineCanRunJob(slaveHost, jobKey) && jobCanRunOnMachine(slaveHost, jobKey);
        }
      };
    }

    private static Set<String> getAvoidJobs(TaskState state) {
      TwitterTaskInfo task = state.task.getAssignedTask().getTask();
      return task.getAvoidJobsSize() > 0 ? task.getAvoidJobs() : ImmutableSet.<String>of();
    }

    /**
     * Creates a filter that identifies tasks that are configuration-compatible with another task.
     *
     * @param taskA Task to find compatible tasks for.
     * @return A filte to find tasks compatible with {@code taskA}.
     */
    private static Predicate<TaskState> canRunWith(final TaskState taskA) {
      final Set<String> taskAAvoids = getAvoidJobs(taskA);

      return new Predicate<TaskState>() {
        @Override public boolean apply(TaskState taskB) {
          Set<String> taskBAvoids = getAvoidJobs(taskB);

          return !taskAAvoids.contains(Tasks.jobKey(taskB))
                 && !taskBAvoids.contains(Tasks.jobKey(taskA));
        }
      };
    }

    private Predicate<TaskState> isTaskAllowedWithResidents(String slaveHost) {
      final Multimap<String, TaskState> tasksOnHostByJob = Multimaps.index(scheduler.getTasks(
          new Query(new TaskQuery().setSlaveHost(slaveHost), Tasks.ACTIVE_FILTER)),
          Tasks.STATE_TO_JOB_KEY);

      return new Predicate<TaskState>() {
        @Override public boolean apply(TaskState state) {
          Collection<TaskState> tasks = tasksOnHostByJob.get(Tasks.jobKey(state));

          int maxPerHost = !state.task.getAssignedTask().getTask().isSetMaxPerHost() ? 1
              : state.task.getAssignedTask().getTask().getMaxPerHost();

          return (tasks != null) && (tasks.size() < maxPerHost)
                 && Iterables.all(tasksOnHostByJob.values(), canRunWith(state));
        }
      };
    }

    private Predicate<TaskState> offerSatisfiesTask(final TwitterTaskInfo offer) {
      return new Predicate<TaskState>() {
        @Override public boolean apply(TaskState state) {
          return ConfigurationManager.satisfied(state.task.getAssignedTask().getTask(), offer);
        }
      };
    }

    // TODO(wfarner): Comparing strings as canonical host IDs could be problematic.  Consider
    //    an approach that would be robust when presented with an IP address as well.
    @Override public Predicate<TaskState> makeFilter(final TwitterTaskInfo resourceOffer,
        final String slaveHost) {
      final Predicate<TaskState> offerSatisfiesTask = offerSatisfiesTask(resourceOffer);
      final Predicate<TaskState> isTaskAllowedWith = isTaskAllowedWithResidents(slaveHost);
      final Predicate<TaskState> isPairAllowed = meetsMachineReservation(slaveHost);

      return new Predicate<TaskState>() {
        @Override public boolean apply(TaskState state) {
          return Predicates.and(offerSatisfiesTask, isTaskAllowedWith, isPairAllowed).apply(state);
        }
      };
    }
  }
}
