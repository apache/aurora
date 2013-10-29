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
package com.twitter.aurora.scheduler.stats;

import java.util.Map;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import com.twitter.aurora.gen.Quota;
import com.twitter.aurora.scheduler.quota.Quotas;
import com.twitter.aurora.scheduler.storage.entities.IQuota;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A stat computer that aggregates the number of 'slots' available at different pre-determined
 * slot sizes.
 */
class SlotSizeCounter implements Runnable {
  private static final Map<String, IQuota> SLOT_SIZES = ImmutableMap.of(
      "small", IQuota.build(new Quota(1.0, 1024, 4096)),
      "medium", IQuota.build(new Quota(4.0, 8192, 16384)),
      "large", IQuota.build(new Quota(8.0, 16384, 32768)),
      "xlarge", IQuota.build(new Quota(16.0, 32768, 65536)));

  private final Map<String, IQuota> slotSizes;
  private final ResourceSlotProvider resourceSlotProvider;
  private final CachedCounters cachedCounters;

  @VisibleForTesting
  SlotSizeCounter(
      final Map<String, IQuota> slotSizes,
      ResourceSlotProvider resourceSlotProvider,
      CachedCounters cachedCounters) {

    this.slotSizes = checkNotNull(slotSizes);
    this.resourceSlotProvider = checkNotNull(resourceSlotProvider);
    this.cachedCounters = checkNotNull(cachedCounters);
  }

  interface ResourceSlotProvider {
    Iterable<IQuota> get();
  }

  @Inject
  SlotSizeCounter(ResourceSlotProvider resourceSlotProvider, CachedCounters cachedCounters) {
    this(SLOT_SIZES, resourceSlotProvider, cachedCounters);
  }

  @VisibleForTesting
  static String getStatName(String slotName) {
    return "empty_slots_" + slotName;
  }

  private int countSlots(Iterable<IQuota> slots, final IQuota slotSize) {
    Function<IQuota, Integer> counter = new Function<IQuota, Integer>() {
      @Override public Integer apply(IQuota machineSlack) {
        return Quotas.divide(machineSlack, slotSize);
      }
    };

    int sum = 0;
    for (int slotCount : FluentIterable.from(slots).transform(counter)) {
      sum += slotCount;
    }
    return sum;
  }

  @Override
  public void run() {
    Iterable<IQuota> slots = resourceSlotProvider.get();
    for (Map.Entry<String, IQuota> entry : slotSizes.entrySet()) {
      cachedCounters.get(getStatName(entry.getKey())).set(countSlots(slots, entry.getValue()));
    }
  }
}
