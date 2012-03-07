package com.twitter.mesos.scheduler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.name.Named;

import com.twitter.common.quantity.Amount;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work.Quiet;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.quantity.Data.BYTES;
import static com.twitter.common.quantity.Data.MB;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;

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

  @VisibleForTesting static final Veto CPU = new Veto("Insufficient CPU");
  @VisibleForTesting static final Veto RAM = new Veto("Insufficient RAM");
  @VisibleForTesting static final Veto DISK = new Veto("Insufficient disk");
  @VisibleForTesting static final Veto PORTS = new Veto("Insufficient ports");
  @VisibleForTesting static final Veto RESTRICTED_JOB =
      new Veto("Job is restricted to other hosts");
  @VisibleForTesting static final Veto RESERVED_HOST =
      new Veto("Host is restricted to another job");
  @VisibleForTesting static final Veto DEDICATED_HOST_VETO = new Veto("Host is dedicated");

  private static final Optional<Veto> NO_VETO = Optional.absent();

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

  /**
   * A function that fetches attributes associated with a given host.
   */
  public interface AttributeLoader extends Function<String, Iterable<Attribute>> { }

  /**
   * A function that may veto a task.
   */
  private interface FilterRule extends Function<TwitterTaskInfo, Iterable<Optional<Veto>>> { }

  /**
   * Convenience class for a rule that will only ever have a single veto.
   */
  private abstract static class SingleVetoRule implements FilterRule {
    @Override public final Iterable<Optional<Veto>> apply(TwitterTaskInfo task) {
      return ImmutableList.of(doApply(task));
    }

    abstract Optional<Veto> doApply(TwitterTaskInfo task);
  }

  private Iterable<FilterRule> rulesFromOffer(final Resources available,
      final Optional<String> slaveHost) {

    return ImmutableList.<FilterRule>of(
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            return (available.getNumCpus() >= task.getNumCpus()) ? NO_VETO : Optional.of(CPU);
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            boolean passed =
                available.getRam().as(BYTES) >= Amount.of(task.getRamMb(), MB).as(BYTES);
            return passed ? NO_VETO : Optional.of(RAM);
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            boolean passed =
                available.getDisk().as(BYTES) >= Amount.of(task.getDiskMb(), MB).as(BYTES);
            return passed ? NO_VETO : Optional.of(DISK);
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            boolean passed = available.getNumPorts() >= task.getRequestedPorts().size();
            return passed ? NO_VETO : Optional.of(PORTS);
          }
        },
        new SingleVetoRule() {
          @Override Optional<Veto> doApply(TwitterTaskInfo task) {
            return slaveHost.isPresent()
                ? checkMachineRestrictions(slaveHost.get(), Tasks.jobKey(task))
                : NO_VETO;
          }
        }
    );
  }

  private Optional<Veto> checkMachineRestrictions(String host, String jobKey) {
    // Legacy check to handle old-style machine reservations.
    String hostReservation = machineRestrictions.get(host);
    if (hostReservation != null) {
      if (!hostReservation.equals(jobKey)) {
        return Optional.of(RESERVED_HOST);
      }
    } else if (!jobCanRunOnMachine(host, jobKey)) {
      return Optional.of(RESTRICTED_JOB);
    }

    return Optional.absent();
  }

  private FilterRule getConstraintFilter(final String slaveHost) {
    return new FilterRule() {
      @Override public Iterable<Optional<Veto>> apply(final TwitterTaskInfo task) {
        if (!task.isSetConstraints()) {
          return ImmutableList.of(Optional.<Veto>absent());
        }

        return storage.doInTransaction(new Quiet<Iterable<Optional<Veto>>>() {
          @Override public Iterable<Optional<Veto>> apply(final StoreProvider storeProvider) {
            AttributeLoader attributeLoader = new AttributeLoader() {
              @Override public Iterable<Attribute> apply(String host) {
                return storeProvider.getAttributeStore().getHostAttributes(host);
              }
            };

            Supplier<Collection<ScheduledTask>> activeTasksSupplier =
                Suppliers.memoize(new Supplier<Collection<ScheduledTask>>() {
                  @Override public Collection<ScheduledTask> get() {
                    return storeProvider.getTaskStore().fetchTasks(
                        Query.activeQuery(Tasks.jobKey(task)));
                  }
                });

            return Iterables.transform(task.getConstraints(),
                new ConstraintFilter(
                    Tasks.jobKey(task),
                    activeTasksSupplier,
                    attributeLoader,
                    attributeLoader.apply(slaveHost)));
          }
        });
      }
    };
  }

  private Iterable<Veto> applyRules(Iterable<FilterRule> rules, TwitterTaskInfo task) {
    ImmutableList.Builder<Veto> builder = ImmutableList.builder();
    for (FilterRule rule: rules) {
      builder.addAll(Optional.presentInstances(rule.apply(task)));
    }
    return builder.build();
  }

  private boolean isDedicated(final String slaveHost) {
    Iterable<Attribute> slaveAttributes = storage.doInTransaction(new Quiet<Iterable<Attribute>>() {
      @Override public Iterable<Attribute> apply(final StoreProvider storeProvider) {
        return storeProvider.getAttributeStore().getHostAttributes(slaveHost);
      }
    });

    return Iterables.any(slaveAttributes, new ConstraintFilter.NameFilter(DEDICATED_ATTRIBUTE));
  }

  @Override
  public Set<Veto> filter(Resources resourceOffer, Optional<String> slaveHost,
      TwitterTaskInfo task) {

    final Iterable<FilterRule> staticRules = rulesFromOffer(resourceOffer, slaveHost);

    Iterable<Veto> staticVetos = applyRules(staticRules, task);
    if (!Iterables.isEmpty(staticVetos)) {
      return ImmutableSet.copyOf(staticVetos);
    } else if (slaveHost.isPresent()) {
      Set<Veto> vetoes;
      if (!ConfigurationManager.isDedicated(task) && isDedicated(slaveHost.get())) {
        vetoes = ImmutableSet.of(DEDICATED_HOST_VETO);
      } else {
        vetoes = ImmutableSet.copyOf(
            Optional.presentInstances(getConstraintFilter(slaveHost.get()).apply(task)));
      }

      return vetoes;
    } else {
      return ImmutableSet.of();
    }
  }
}
