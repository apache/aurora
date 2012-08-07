package com.twitter.mesos.scheduler;

import java.util.Collection;
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
 */
public class SchedulingFilterImpl implements SchedulingFilter {

  @VisibleForTesting static final Veto CPU = new Veto("Insufficient CPU");
  @VisibleForTesting static final Veto RAM = new Veto("Insufficient RAM");
  @VisibleForTesting static final Veto DISK = new Veto("Insufficient disk");
  @VisibleForTesting static final Veto PORTS = new Veto("Insufficient ports");
  @VisibleForTesting static final Veto DEDICATED_HOST_VETO = new Veto("Host is dedicated");

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

  private Iterable<FilterRule> rulesFromOffer(final Resources available) {
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
        }
    );
  }

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

            return Optional.presentInstances(Iterables.transform(task.getConstraints(),
                new ConstraintFilter(
                    Tasks.jobKey(task),
                    activeTasksSupplier,
                    attributeLoader,
                    attributeLoader.apply(slaveHost))));
          }
        });
      }
    };
  }

  private Iterable<Veto> applyRules(Iterable<FilterRule> rules, TwitterTaskInfo task) {
    ImmutableList.Builder<Veto> builder = ImmutableList.builder();
    for (FilterRule rule: rules) {
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
  public Set<Veto> filter(Resources resourceOffer, String slaveHost, TwitterTaskInfo task) {
    Iterable<Veto> staticVetos = applyRules(rulesFromOffer(resourceOffer), task);
    if (!Iterables.isEmpty(staticVetos)) {
      return ImmutableSet.copyOf(staticVetos);
    }
    Set<Veto> vetoes;
    if (!ConfigurationManager.isDedicated(task) && isDedicated(slaveHost)) {
      vetoes = ImmutableSet.of(DEDICATED_HOST_VETO);
    } else {
      vetoes = ImmutableSet.copyOf(getConstraintFilter(slaveHost).apply(task));
    }

    return vetoes;
  }
}
