/*
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.filter;

import java.util.Collection;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;

import javax.inject.Inject;

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

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.state.MaintenanceController;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.aurora.gen.MaintenanceMode.DRAINED;
import static org.apache.aurora.gen.MaintenanceMode.DRAINING;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.CPU;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.DISK;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.PORTS;
import static org.apache.aurora.scheduler.filter.SchedulingFilterImpl.ResourceVector.RAM;

/**
 * Implementation of the scheduling filter that ensures resource requirements of tasks are
 * fulfilled, and that tasks are allowed to run on the given machine.
 *
 */
public class SchedulingFilterImpl implements SchedulingFilter {

  @VisibleForTesting static final Veto DEDICATED_HOST_VETO =
      Veto.constraintMismatch("Host is dedicated");

  private static final Optional<Veto> NO_VETO = Optional.absent();

  private static final Set<MaintenanceMode> VETO_MODES = EnumSet.of(DRAINING, DRAINED);

  private final Storage storage;
  private final MaintenanceController maintenance;

  /**
   * Creates a new scheduling filter.
   *
   * @param storage Interface to accessing the task store.
   * @param maintenance Interface to accessing the maintenance controller
   */
  @Inject
  public SchedulingFilterImpl(Storage storage, MaintenanceController maintenance) {
    this.storage = checkNotNull(storage);
    this.maintenance = checkNotNull(maintenance);
  }

  /**
   * A function that fetches attributes associated with a given host.
   */
  public interface AttributeLoader extends Function<String, Iterable<Attribute>> { }

  /**
   * A function that may veto a task.
   */
  private interface FilterRule extends Function<ITaskConfig, Iterable<Veto>> { }

  /**
   * Convenience class for a rule that will only ever have a single veto.
   */
  private abstract static class SingleVetoRule implements FilterRule {
    @Override public final Iterable<Veto> apply(ITaskConfig task) {
      return doApply(task).asSet();
    }

    abstract Optional<Veto> doApply(ITaskConfig task);
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
    private final int range;
    @VisibleForTesting
    int getRange() {
      return range;
    }

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

  private Iterable<FilterRule> rulesFromOffer(final ResourceSlot available) {
    return ImmutableList.<FilterRule>of(
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(ITaskConfig task) {
            return CPU.maybeVeto(
                available.getNumCpus(),
                ResourceSlot.from(task).getNumCpus());
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(ITaskConfig task) {
            return RAM.maybeVeto(
                available.getRam().as(Data.MB),
                ResourceSlot.from(task).getRam().as(Data.MB));
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(ITaskConfig task) {
            return DISK.maybeVeto(available.getDisk().as(Data.MB),
                ResourceSlot.from(task).getDisk().as(Data.MB));
          }
        },
        new SingleVetoRule() {
          @Override public Optional<Veto> doApply(ITaskConfig task) {
            return PORTS.maybeVeto(available.getNumPorts(),
                ResourceSlot.from(task).getNumPorts());
          }
        }
    );
  }

  private static boolean isValueConstraint(IConstraint constraint) {
    return constraint.getConstraint().getSetField() == TaskConstraint._Fields.VALUE;
  }

  private static final Ordering<IConstraint> VALUES_FIRST = Ordering.from(
      new Comparator<IConstraint>() {
        @Override public int compare(IConstraint a, IConstraint b) {
          if (a.getConstraint().getSetField() == b.getConstraint().getSetField()) {
            return 0;
          }
          return isValueConstraint(a) ? -1 : 1;
        }
      });

  private static final Iterable<ScheduleStatus> ACTIVE_NOT_PENDING_STATES =
      EnumSet.copyOf(Sets.difference(Tasks.ACTIVE_STATES, EnumSet.of(ScheduleStatus.PENDING)));

  private FilterRule getConstraintFilter(final String slaveHost) {
    return new FilterRule() {
      @Override public Iterable<Veto> apply(final ITaskConfig task) {
        if (!task.isSetConstraints()) {
          return ImmutableList.of();
        }

        // In the interest of performance, we perform a weakly consistent read here.  The biggest
        // risk of this is that we might schedule against stale host attributes, or we might fail
        // to correctly satisfy a diversity constraint.  Given that the likelihood is relatively low
        // for both of these, and the impact is also low, the weak consistency is acceptable.
        return storage.weaklyConsistentRead(new Quiet<Iterable<Veto>>() {
          @Override public Iterable<Veto> apply(final StoreProvider storeProvider) {
            AttributeLoader attributeLoader = new AttributeLoader() {
              @Override public Iterable<Attribute> apply(String host) {
                return AttributeStore.Util.attributesOrNone(storeProvider, host);
              }
            };

            Supplier<Collection<IScheduledTask>> activeTasksSupplier =
                Suppliers.memoize(new Supplier<Collection<IScheduledTask>>() {
                  @Override public Collection<IScheduledTask> get() {
                    return storeProvider.getTaskStore().fetchTasks(
                        Query.jobScoped(Tasks.INFO_TO_JOB_KEY.apply(task))
                            .byStatus(ACTIVE_NOT_PENDING_STATES));
                  }
                });

            ConstraintFilter constraintFilter = new ConstraintFilter(
                Tasks.INFO_TO_JOB_KEY.apply(task),
                activeTasksSupplier,
                attributeLoader,
                attributeLoader.apply(slaveHost));
            ImmutableList.Builder<Veto> vetoes = ImmutableList.builder();
            for (IConstraint constraint : VALUES_FIRST.sortedCopy(task.getConstraints())) {
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

  private Optional<Veto> getMaintenanceVeto(String slaveHost) {
    MaintenanceMode mode = maintenance.getMode(slaveHost);
    return VETO_MODES.contains(mode)
        ? Optional.of(ConstraintFilter.maintenanceVeto(mode.toString().toLowerCase()))
        : NO_VETO;
  }

  private Set<Veto> getResourceVetoes(ResourceSlot offer, ITaskConfig task) {
    ImmutableSet.Builder<Veto> builder = ImmutableSet.builder();
    for (FilterRule rule : rulesFromOffer(offer)) {
      builder.addAll(rule.apply(task));
    }
    return builder.build();
  }

  private boolean isDedicated(final String slaveHost) {
    Iterable<Attribute> slaveAttributes =
        storage.weaklyConsistentRead(new Quiet<Iterable<Attribute>>() {
          @Override public Iterable<Attribute> apply(final StoreProvider storeProvider) {
            return AttributeStore.Util.attributesOrNone(storeProvider, slaveHost);
          }
        });

    return Iterables.any(slaveAttributes, new ConstraintFilter.NameFilter(DEDICATED_ATTRIBUTE));
  }

  @Override
  public Set<Veto> filter(ResourceSlot offer, String slaveHost, ITaskConfig task, String taskId) {
    if (!ConfigurationManager.isDedicated(task) && isDedicated(slaveHost)) {
      return ImmutableSet.of(DEDICATED_HOST_VETO);
    }
    return ImmutableSet.<Veto>builder()
        .addAll(getConstraintFilter(slaveHost).apply(task))
        .addAll(getResourceVetoes(offer, task))
        .addAll(getMaintenanceVeto(slaveHost).asSet())
        .build();
  }
}
