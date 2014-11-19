/**
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

import java.util.Comparator;
import java.util.EnumSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.storage.entities.IAttribute;
import org.apache.aurora.scheduler.storage.entities.IConstraint;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;

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
 */
public class SchedulingFilterImpl implements SchedulingFilter {

  @VisibleForTesting static final Veto DEDICATED_HOST_VETO =
      Veto.constraintMismatch("Host is dedicated");

  private static final Optional<Veto> NO_VETO = Optional.absent();

  private static final Set<MaintenanceMode> VETO_MODES = EnumSet.of(DRAINING, DRAINED);

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
      return Math.min(Veto.MAX_SCORE, (int) (Veto.MAX_SCORE * value) / range);
    }

    @VisibleForTesting
    Veto veto(double excess) {
      return new Veto("Insufficient " + name, scale(excess, range));
    }
  }

  private static void maybeAddVeto(
      ImmutableList.Builder<Veto> vetoes,
      ResourceVector vector,
      double available,
      double requested) {

    Optional<Veto> veto = vector.maybeVeto(available, requested);
    if (veto.isPresent()) {
      vetoes.add(veto.get());
    }
  }

  private static Iterable<Veto> getResourceVetoes(ResourceSlot available, ResourceSlot required) {
    ImmutableList.Builder<Veto> vetoes = ImmutableList.builder();
    maybeAddVeto(vetoes, CPU, available.getNumCpus(), required.getNumCpus());
    maybeAddVeto(vetoes, RAM, available.getRam().as(Data.MB), required.getRam().as(Data.MB));
    maybeAddVeto(vetoes, DISK, available.getDisk().as(Data.MB), required.getDisk().as(Data.MB));
    maybeAddVeto(vetoes, PORTS, available.getNumPorts(), required.getNumPorts());
    return vetoes.build();
  }

  private static boolean isValueConstraint(IConstraint constraint) {
    return constraint.getConstraint().getSetField() == TaskConstraint._Fields.VALUE;
  }

  private static final Ordering<IConstraint> VALUES_FIRST = Ordering.from(
      new Comparator<IConstraint>() {
        @Override
        public int compare(IConstraint a, IConstraint b) {
          if (a.getConstraint().getSetField() == b.getConstraint().getSetField()) {
            return 0;
          }
          return isValueConstraint(a) ? -1 : 1;
        }
      });

  private Iterable<Veto> getConstraintVetoes(
      Iterable<IConstraint> taskConstraints,
      AttributeAggregate jobState,
      Iterable<IAttribute> offerAttributes) {

    ImmutableList.Builder<Veto> vetoes = ImmutableList.builder();
    for (IConstraint constraint : VALUES_FIRST.sortedCopy(taskConstraints)) {
      Optional<Veto> veto = ConstraintMatcher.getVeto(jobState, offerAttributes, constraint);
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

  private Optional<Veto> getMaintenanceVeto(MaintenanceMode mode) {
    return VETO_MODES.contains(mode)
        ? Optional.of(ConstraintMatcher.maintenanceVeto(mode.toString().toLowerCase()))
        : NO_VETO;
  }

  private boolean isDedicated(IHostAttributes attributes) {
    return Iterables.any(
        attributes.getAttributes(),
        new ConstraintMatcher.NameFilter(DEDICATED_ATTRIBUTE));
  }

  @Override
  public Set<Veto> filter(UnusedResource resource, ResourceRequest request) {
    if (!ConfigurationManager.isDedicated(request.getConstraints())
        && isDedicated(resource.getAttributes())) {

      return ImmutableSet.of(DEDICATED_HOST_VETO);
    }

    Optional<Veto> maintenanceVeto = getMaintenanceVeto(resource.getAttributes().getMode());
    if (maintenanceVeto.isPresent()) {
      return maintenanceVeto.asSet();
    }

    return ImmutableSet.<Veto>builder()
        .addAll(getConstraintVetoes(
            request.getConstraints(),
            request.getJobState(),
            resource.getAttributes().getAttributes()))
        .addAll(getResourceVetoes(resource.getResourceSlot(), request.getResourceSlot()))
        .build();
  }
}
