package com.twitter.mesos.scheduler;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implementation of the scheduling filter that ensures resource requirements of tasks are
 * fulfilled, and that tasks are allowed to run on the given machine.
 *
 * TODO(wfarner): A big improvement on this would be allowing non-matched constraints to bubble
 *     up to the caller, so that we may expose constraints that are not being matched as a reason
 *     for a task not being scheduled.
 */
public class SchedulingFilterImpl implements SchedulingFilter {

  /**
   * {@literal @Named} binding key for the machine reservation map.
   */
  public static final String MACHINE_RESTRICTIONS =
      "com.twitter.mesos.scheduler.MACHINE_RESTRICTIONS";

  private final Map<String, String> machineRestrictions;
  private final Storage storage;

  /**
   * Creates a new scheduling filter.
   * TODO(wfarner): Use host attribute to implement machine restrictions.
   *
   * @param machineRestrictions Mapping from machine host name to job key.  Restricted machines
   *    may only run the specified job key, and the specified job key may only run on the
   *    respective machine.
   * @param storage Interface to accessing the task store.
   */
  @Inject
  public SchedulingFilterImpl(
      @Named(MACHINE_RESTRICTIONS) Map<String, String> machineRestrictions, Storage storage) {
    this.machineRestrictions = checkNotNull(machineRestrictions);
    this.storage = checkNotNull(storage);
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
        if (hostToJob.getKey().equals(slaveHost)) {
          return true;
        }
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

  private static Predicate<TwitterTaskInfo> offerSatisfiesTask(final Resources offer) {
    return new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(TwitterTaskInfo task) {
        return offer.greaterThanOrEqual(Resources.from(task));
      }
    };
  }

  @Override
  public Predicate<TwitterTaskInfo> staticFilter(Resources resourceOffer,
      @Nullable String slaveHost) {
    if (slaveHost == null) {
      return offerSatisfiesTask(resourceOffer);
    }
    // TODO(wfarner): This should also match value attribute constraints.
    return Predicates.and(offerSatisfiesTask(resourceOffer), meetsMachineReservation(slaveHost));
  }

  public interface AttributeLoader extends Function<String, Iterable<Attribute>> {}

  @Override
  public Predicate<TwitterTaskInfo> dynamicFilter(final String slaveHost) {
    return new Predicate<TwitterTaskInfo>() {
      @Override public boolean apply(final TwitterTaskInfo taskInfo) {
        return storage.doInTransaction(new Work.Quiet<Boolean>() {
          @Override public Boolean apply(final StoreProvider storeProvider) {
            // We can schedule this task if there's no constraint.
            // TODO(wfarner): This needs to be fixed - e.g. if there is a dedicated attribute
            // on the host, we cannot allow the task to run.
            if (!taskInfo.isSetConstraints()) {
              return true;
            }

            AttributeLoader attributeLoader = new AttributeLoader() {
              @Override public Iterable<Attribute> apply(String host) {
                return storeProvider.getAttributeStore().getAttributeForHost(host);
              }
            };

            Supplier<Collection<ScheduledTask>> activeTasksSupplier =
                Suppliers.memoize(new Supplier<Collection<ScheduledTask>>() {
                  @Override public Collection<ScheduledTask> get() {
                    return storeProvider.getTaskStore().fetchTasks(
                        Query.activeQuery(Tasks.jobKey(taskInfo)));
                  }
                });
            return Iterables.all(taskInfo.getConstraints(),
                new ConstraintFilter(
                    Tasks.jobKey(taskInfo),
                    activeTasksSupplier,
                    attributeLoader,
                    attributeLoader.apply(slaveHost)));
          }
        });
      }
    };
  }
}
