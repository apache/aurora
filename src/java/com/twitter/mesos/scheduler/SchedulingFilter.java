package com.twitter.mesos.scheduler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Determines whether a proposed scheduling assignment should be allowed.
 *
 * @author William Farner
 */
public interface SchedulingFilter {

  /**
   * Creates a filter that checks static state to ensure that task configurations meet a
   * resource offer for a given slave.  The returned filter will not check the configuration against
   * any dynamic state.
   *
   * @param resourceOffer The resource offer to check against tasks.
   * @param slaveHost The slave host that the resource offer is associated with.
   * @return A new predicate that can be used to find tasks satisfied by the offer.
   */
  public Predicate<TwitterTaskInfo> staticFilter(Resources resourceOffer, String slaveHost);

  /**
   * Creates a filter that will make decisions about whether tasks are allowed to run on a machine,
   * by checking things such as the active tasks on the machine.
   *
   * @param taskFetcher Resource to look up existing task information.
   * @param slaveHost The slave host to filter tasks against.
   * @return A new predicate that can be used to find tasks satisfied by the state of
   *     {@code slaveHost}.
   */
  public Predicate<TwitterTaskInfo> dynamicHostFilter(
      Function<Query, Iterable<TwitterTaskInfo>> taskFetcher, String slaveHost);

  /**
   * Implementation of the scheduling filter that ensures resource requirements of tasks are
   * fulfilled, and that tasks are allowed to run on the given machine.
   */
  public static class SchedulingFilterImpl implements SchedulingFilter {

    /**
     * {@literal @Named} binding key for the machine reservation map.
     */
    public static final String MACHINE_RESTRICTIONS =
        "com.twitter.mesos.scheduler.MACHINE_RESTRICTIONS";

    private final Map<String, String> machineRestrictions;

    /**
     * Creates a new scheduling filter.
     *
     * @param machineRestrictions Mapping from machine host name to job key.  Restricted machines
     *    may only run the specified job key, and the specified job key may only run on the
     *    respective machine.
     */
    @Inject
    public SchedulingFilterImpl(
        @Named(MACHINE_RESTRICTIONS) Map<String, String> machineRestrictions) {
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

    private Predicate<TwitterTaskInfo> meetsMachineReservation(final String slaveHost) {
      return new Predicate<TwitterTaskInfo>() {
        @Override public boolean apply(TwitterTaskInfo task) {
          String jobKey = Tasks.jobKey(task);
          return machineCanRunJob(slaveHost, jobKey) && jobCanRunOnMachine(slaveHost, jobKey);
        }
      };
    }

    private static Set<String> getAvoidJobs(TwitterTaskInfo task) {
      return task.getAvoidJobsSize() > 0 ? task.getAvoidJobs() : ImmutableSet.<String>of();
    }

    /**
     * Creates a filter that identifies tasks that are configuration-compatible with another task.
     *
     * @param taskA Task to find compatible tasks for.
     * @return A filte to find tasks compatible with {@code taskA}.
     */
    private static Predicate<TwitterTaskInfo> canRunWith(final TwitterTaskInfo taskA) {
      final Set<String> taskAAvoids = getAvoidJobs(taskA);

      return new Predicate<TwitterTaskInfo>() {
        @Override public boolean apply(TwitterTaskInfo taskB) {
          Set<String> taskBAvoids = getAvoidJobs(taskB);

          return !taskAAvoids.contains(Tasks.jobKey(taskB))
              && !taskBAvoids.contains(Tasks.jobKey(taskA));
        }
      };
    }

    private static Predicate<TwitterTaskInfo> offerSatisfiesTask(final Resources offer) {
      return new Predicate<TwitterTaskInfo>() {
        @Override public boolean apply(TwitterTaskInfo task) {
          return offer.greaterThanOrEqual(Resources.from(task));
        }
      };
    }

    @Override
    public Predicate<TwitterTaskInfo> staticFilter(Resources resourceOffer,
        String slaveHost) {
      final Predicate<TwitterTaskInfo> offerSatisfiesTask = offerSatisfiesTask(resourceOffer);
      final Predicate<TwitterTaskInfo> isPairAllowed = meetsMachineReservation(slaveHost);

      return new Predicate<TwitterTaskInfo>() {
        @Override public boolean apply(TwitterTaskInfo task) {
          return Predicates.and(ImmutableList.<Predicate<TwitterTaskInfo>>builder()
              .add(offerSatisfiesTask)
              .add(isPairAllowed)
              .build())
              .apply(task);
        }
      };
    }

    @Override
    public Predicate<TwitterTaskInfo> dynamicHostFilter(
        final Function<Query, Iterable<TwitterTaskInfo>> taskFetcher, final String slaveHost) {
      // TODO(William Farner): Comparing strings as canonical host IDs could be problematic.
      //     Consider an approach that would be robust when presented with an IP address as well.

      final Supplier<Multimap<String, TwitterTaskInfo>> tasksOnHostByJobSupplier =
          Suppliers.memoize(new Supplier<Multimap<String, TwitterTaskInfo>>() {
            @Override public Multimap<String, TwitterTaskInfo> get() {
              Iterable<TwitterTaskInfo> tasks = taskFetcher.apply(
                  new Query(new TaskQuery().setSlaveHost(slaveHost), Tasks.ACTIVE_FILTER));
              return Multimaps.index(tasks, Tasks.INFO_TO_JOB_KEY);
            }
          });

      return new Predicate<TwitterTaskInfo>() {
        @Override public boolean apply(TwitterTaskInfo task) {
          Collection<TwitterTaskInfo> tasks =
              tasksOnHostByJobSupplier.get().get(Tasks.jobKey(task));
          int maxPerHost =
              (!task.isSetMaxPerHost() || task.getMaxPerHost() == 0) ? 1 : task.getMaxPerHost();

          return (tasks != null) && (tasks.size() < maxPerHost)
                 && Iterables.all(tasksOnHostByJobSupplier.get().values(), canRunWith(task));
        }
      };
    }
  }
}
