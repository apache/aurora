package com.twitter.mesos.scheduler;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.ConfigurationManager;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work.Quiet;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.scheduler.SchedulingFilterImpl.ResourceVector.CPU;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.ResourceVector.DISK;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.ResourceVector.PORTS;
import static com.twitter.mesos.scheduler.SchedulingFilterImpl.ResourceVector.RAM;
import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;

/**
 * Implementation of the scheduling filter that ensures resource requirements of tasks are
 * fulfilled, and that tasks are allowed to run on the given machine.
 *
 */
public class SchedulingFilterImpl implements SchedulingFilter {

  @VisibleForTesting static final Veto DEDICATED_HOST_VETO =
      Veto.constraintMismatch("Host is dedicated");

  private static final Optional<Veto> NO_VETO = Optional.absent();

  private final Storage storage;

  /**
   * Creates a new scheduling filter.
   *
   * @param storage Interface to accessing the task store.
   */
  @Inject
  public SchedulingFilterImpl(Storage storage) {
    this.storage = checkNotNull(storage);
  }

  /**
   * A function that fetches attributes associated with a given host.
   */
  public interface AttributeLoader extends Function<String, Iterable<Attribute>> { }

  /**
   * A function that may veto a task.
   */
  private interface FilterRule extends Function<TwitterTaskInfo, Iterable<Veto>> { }

  /**
   * Convenience class for a rule that will only ever have a single veto.
   */
  private abstract static class SingleVetoRule implements FilterRule {
    @Override public final Iterable<Veto> apply(TwitterTaskInfo task) {
      return doApply(task).asSet();
    }

    abstract Optional<Veto> doApply(TwitterTaskInfo task);
  }

  // Scaling ranges to use for comparison of vetos.  This has no real bearing besides trying to
  // determine if a veto along one resource vector is a 'stronger' veto than that of another vector.
  // The values below represent the maximum resources on a typical slave machine.
  @VisibleForTesting
  enum ResourceVector {
    CPU("CPU", 16),
    RAM("RAM", Amount.of(24, Data.GB).as(Data.MB)),
    DISK("disk", Amount.of(450, Data.GB).as(Data.MB)),
    PORTS("ports", 1000);

    private final String name;
    @VisibleForTesting
    final int range;

    private ResourceVector(String name, int range) {
      this.name = name;
      this.range = range;
    }

    Optional<Veto> maybeVeto(double available, double requested) {
      double tooLarge = requested - available;
      if (tooLarge <= 0) {
        return NO_VETO;
      } else {
        return Optional.of(veto(tooLarge));
      }
    }

    private static int scale(double value, int range) {
      return Math.min(Veto.MAX_SCORE, (int) ((Veto.MAX_SCORE * value)) / range);
    }

    @VisibleForTesting
    Veto veto(double excess) {
      return new Veto("Insufficient " + name, scale(excess, range));
    }
  }

  private Iterable<FilterRule> rulesFromOffer(final Resources available) {
    return ImmutableList.<FilterRule>of(
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            return CPU.maybeVeto(available.getNumCpus(), ThermosResources.getTotalTaskCpus(task));
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            return RAM.maybeVeto(available.getRam().as(Data.MB),
                ThermosResources.getTotalTaskRam(task).as(Data.MB));
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            return DISK.maybeVeto(available.getDisk().as(Data.MB), task.getDiskMb());
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(TwitterTaskInfo task) {
            return PORTS.maybeVeto(available.getNumPorts(), task.getRequestedPorts().size());
          }
        }
    );
  }

  private static boolean isValueConstraint(Constraint constraint) {
    return constraint.getConstraint().isSet(TaskConstraint._Fields.VALUE);
  }

  private static final Ordering<Constraint> VALUES_FIRST = Ordering.from(
      new Comparator<Constraint>() {
        @Override public int compare(Constraint a, Constraint b) {
          if (a.getConstraint().getSetField() == b.getConstraint().getSetField()) {
            return 0;
          }
          return isValueConstraint(a) ? -1 : 1;
        }
      });

  private FilterRule getConstraintFilter(final String slaveHost) {
    return new FilterRule() {
      @Override public Iterable<Veto> apply(final TwitterTaskInfo task) {
        if (!task.isSetConstraints()) {
          return ImmutableList.of();
        }

        return storage.doInTransaction(new Quiet<Iterable<Veto>>() {
          @Override public Iterable<Veto> apply(final StoreProvider storeProvider) {
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

            ConstraintFilter constraintFilter = new ConstraintFilter(
                Tasks.jobKey(task),
                activeTasksSupplier,
                attributeLoader,
                attributeLoader.apply(slaveHost));
            ImmutableList.Builder<Veto> vetoes = ImmutableList.builder();
            for (Constraint constraint : VALUES_FIRST.sortedCopy(task.getConstraints())) {
              Optional<Veto> veto = constraintFilter.apply(constraint);
              if (veto.isPresent()) {
                vetoes.add(veto.get());
                if (isValueConstraint(constraint)) {
                  // Break when a value constraint mismatch is found to avoid other
                  // potentially-expensive operations to satisfy other constraints.
                  break;
                }
              }
            }

            return vetoes.build();
          }
        });
      }
    };
  }

  private Set<Veto> getResourceVetoes(Resources offer, TwitterTaskInfo task) {
    ImmutableSet.Builder<Veto> builder = ImmutableSet.builder();
    for (FilterRule rule : rulesFromOffer(offer)) {
      builder.addAll(rule.apply(task));
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
  public Set<Veto> filter(Resources offer, String slaveHost, TwitterTaskInfo task, String taskId) {
    if (!ConfigurationManager.isDedicated(task) && isDedicated(slaveHost)) {
      return ImmutableSet.of(DEDICATED_HOST_VETO);
    }

    Set<Veto> constraintVetoes = ImmutableSet.copyOf(getConstraintFilter(slaveHost).apply(task));
    return ImmutableSet.copyOf(Sets.union(getResourceVetoes(offer, task), constraintVetoes));
  }
}
