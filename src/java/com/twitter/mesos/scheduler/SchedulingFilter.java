package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;

import java.util.Map;

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
  public Predicate<ScheduledTask> makeFilter(TwitterTaskInfo resourceOffer, String slaveHost);

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
      this.machineRestrictions = Preconditions.checkNotNull(machineRestrictions);
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

    private boolean isPairAllowed(String slaveHost, String jobKey) {
      return machineCanRunJob(slaveHost, jobKey) && jobCanRunOnMachine(slaveHost, jobKey);
    }

    // TODO(wfarner): Comparing strings as canonical host IDs could be problematic.  Consider
    //    an approach that would be robust when presented with an IP address as well.
    @Override public Predicate<ScheduledTask> makeFilter(final TwitterTaskInfo resourceOffer,
        final String slaveHost) {
      return new Predicate<ScheduledTask>() {
        @Override public boolean apply(ScheduledTask task) {
          // First check if the offer actually satisfies the resources required for the task.
          if (!ConfigurationManager.satisfied(task.getAssignedTask().getTask(), resourceOffer)) {
            return false;
          }

          // Now check if the task may be run on the machine.
          return isPairAllowed(slaveHost, Tasks.jobKey(task));
        }
      };
    }
  }
}
