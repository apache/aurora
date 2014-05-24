/**
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
package org.apache.aurora.scheduler.stats;

import java.util.Map;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;

import org.apache.aurora.scheduler.base.ResourceAggregates;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A stat computer that aggregates the number of 'slots' available at different pre-determined
 * slot sizes, broken down by dedicated and non-dedicated hosts.
 */
class SlotSizeCounter implements Runnable {
  private static final Map<String, IResourceAggregate> SLOT_SIZES = ImmutableMap.of(
      "small", ResourceAggregates.SMALL,
      "medium", ResourceAggregates.MEDIUM,
      "large", ResourceAggregates.LARGE,
      "xlarge", ResourceAggregates.XLARGE);

  private final Map<String, IResourceAggregate> slotSizes;
  private final MachineResourceProvider machineResourceProvider;
  private final CachedCounters cachedCounters;

  @VisibleForTesting
  SlotSizeCounter(
      final Map<String, IResourceAggregate> slotSizes,
      MachineResourceProvider machineResourceProvider,
      CachedCounters cachedCounters) {

    this.slotSizes = checkNotNull(slotSizes);
    this.machineResourceProvider = checkNotNull(machineResourceProvider);
    this.cachedCounters = checkNotNull(cachedCounters);
  }

  static class MachineResource {
    private final IResourceAggregate size;
    private final boolean dedicated;

    public MachineResource(IResourceAggregate size, boolean dedicated) {
      this.size = Preconditions.checkNotNull(size);
      this.dedicated = dedicated;
    }

    public IResourceAggregate getSize() {
      return size;
    }

    public boolean isDedicated() {
      return dedicated;
    }
  }

  interface MachineResourceProvider {
    Iterable<MachineResource> get();
  }

  @Inject
  SlotSizeCounter(MachineResourceProvider machineResourceProvider, CachedCounters cachedCounters) {
    this(SLOT_SIZES, machineResourceProvider, cachedCounters);
  }

  @VisibleForTesting
  static String getStatName(String slotName, boolean dedicated) {
    if (dedicated) {
      return "empty_slots_dedicated_" + slotName;
    } else {
      return "empty_slots_" + slotName;
    }
  }

  private int countSlots(Iterable<IResourceAggregate> slots, final IResourceAggregate slotSize) {
    Function<IResourceAggregate, Integer> counter = new Function<IResourceAggregate, Integer>() {
      @Override
      public Integer apply(IResourceAggregate machineSlack) {
        return ResourceAggregates.divide(machineSlack, slotSize);
      }
    };

    int sum = 0;
    for (int slotCount : FluentIterable.from(slots).transform(counter)) {
      sum += slotCount;
    }
    return sum;
  }

  private static Predicate<MachineResource> isDedicated(final boolean dedicated) {
    return new Predicate<MachineResource>() {
      @Override
      public boolean apply(MachineResource slot) {
        return slot.isDedicated() == dedicated;
      }
    };
  }

  private static final Function<MachineResource, IResourceAggregate> GET_SIZE =
      new Function<MachineResource, IResourceAggregate>() {
        @Override
        public IResourceAggregate apply(MachineResource slot) {
          return slot.getSize();
        }
      };

  private void updateStats(
      String name,
      boolean dedicated,
      Iterable<MachineResource> slots,
      IResourceAggregate slotSize) {

    Iterable<IResourceAggregate> sizes =
        FluentIterable.from(slots).filter(isDedicated(dedicated)).transform(GET_SIZE);
    cachedCounters.get(getStatName(name, dedicated)).set(countSlots(sizes, slotSize));
  }

  @Override
  public void run() {
    Iterable<MachineResource> slots = machineResourceProvider.get();
    for (Map.Entry<String, IResourceAggregate> entry : slotSizes.entrySet()) {
      updateStats(entry.getKey(), false, slots, entry.getValue());
      updateStats(entry.getKey(), true, slots, entry.getValue());
    }
  }
}
