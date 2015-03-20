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
package org.apache.aurora.scheduler.async.preemptor;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Qualifier;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.util.Clock;

import org.apache.aurora.scheduler.async.preemptor.PreemptionSlotFinder.PreemptionSlot;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

/**
 * Caches preemption slots found for candidate tasks. Entries are purged from cache after #duration.
 */
class PreemptionSlotCache {

  @VisibleForTesting
  @Qualifier
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  @interface PreemptionSlotHoldDuration { }

  @VisibleForTesting
  static final String PREEMPTION_SLOT_CACHE_SIZE_STAT = "preemption_slot_cache_size";

  private final Cache<String, PreemptionSlotFinder.PreemptionSlot> slots;

  @Inject
  PreemptionSlotCache(
      StatsProvider statsProvider,
      @PreemptionSlotHoldDuration Amount<Long, Time> duration,
      final Clock clock) {

    requireNonNull(duration);
    requireNonNull(clock);
    this.slots = CacheBuilder.newBuilder()
        .expireAfterWrite(duration.as(Time.MINUTES), TimeUnit.MINUTES)
        .ticker(new Ticker() {
          @Override
          public long read() {
            return clock.nowNanos();
          }
        })
        .build();

    statsProvider.makeGauge(
        PREEMPTION_SLOT_CACHE_SIZE_STAT,
        new Supplier<Long>() {
          @Override
          public Long get() {
            return slots.size();
          }
        });
  }

  void add(String taskId, PreemptionSlot preemptionSlot) {
    requireNonNull(taskId);
    requireNonNull(preemptionSlot);
    slots.put(taskId, preemptionSlot);
  }

  Optional<PreemptionSlot> get(String taskId) {
    return Optional.fromNullable(slots.getIfPresent(taskId));
  }

  void remove(String taskId) {
    slots.invalidate(taskId);
  }
}
