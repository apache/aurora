/**
 * Copyright 2013 Apache Software Foundation
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

import org.apache.aurora.gen.Quota;
import org.apache.aurora.scheduler.quota.Quotas;
import org.apache.aurora.scheduler.storage.entities.IQuota;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A stat computer that aggregates the number of 'slots' available at different pre-determined
 * slot sizes, broken down by dedicated and non-dedicated hosts.
 */
class SlotSizeCounter implements Runnable {
  private static final Map<String, IQuota> SLOT_SIZES = ImmutableMap.of(
      "small", IQuota.build(new Quota(1.0, 1024, 4096)),
      "medium", IQuota.build(new Quota(4.0, 8192, 16384)),
      "large", IQuota.build(new Quota(8.0, 16384, 32768)),
      "xlarge", IQuota.build(new Quota(16.0, 32768, 65536)));

  private final Map<String, IQuota> slotSizes;
  private final MachineResourceProvider machineResourceProvider;
  private final CachedCounters cachedCounters;

  @VisibleForTesting
  SlotSizeCounter(
      final Map<String, IQuota> slotSizes,
      MachineResourceProvider machineResourceProvider,
      CachedCounters cachedCounters) {

    this.slotSizes = checkNotNull(slotSizes);
    this.machineResourceProvider = checkNotNull(machineResourceProvider);
    this.cachedCounters = checkNotNull(cachedCounters);
  }

  static class MachineResource {
    private final IQuota size;
    private final boolean dedicated;

    public MachineResource(IQuota size, boolean dedicated) {
      this.size = Preconditions.checkNotNull(size);
      this.dedicated = dedicated;
    }

    public IQuota getSize() {
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

  private int countSlots(Iterable<IQuota> slots, final IQuota slotSize) {
    Function<IQuota, Integer> counter = new Function<IQuota, Integer>() {
      @Override
      public Integer apply(IQuota machineSlack) {
        return Quotas.divide(machineSlack, slotSize);
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

  private static final Function<MachineResource, IQuota> GET_SIZE =
      new Function<MachineResource, IQuota>() {
        @Override
        public IQuota apply(MachineResource slot) {
          return slot.getSize();
        }
      };

  private void updateStats(
      String name,
      boolean dedicated,
      Iterable<MachineResource> slots,
      IQuota slotSize) {

    Iterable<IQuota> sizes =
        FluentIterable.from(slots).filter(isDedicated(dedicated)).transform(GET_SIZE);
    cachedCounters.get(getStatName(name, dedicated)).set(countSlots(sizes, slotSize));
  }

  @Override
  public void run() {
    Iterable<MachineResource> slots = machineResourceProvider.get();
    for (Map.Entry<String, IQuota> entry : slotSizes.entrySet()) {
      updateStats(entry.getKey(), false, slots, entry.getValue());
      updateStats(entry.getKey(), true, slots, entry.getValue());
    }
  }
}
