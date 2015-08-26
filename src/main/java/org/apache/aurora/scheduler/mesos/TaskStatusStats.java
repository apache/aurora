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
package org.apache.aurora.scheduler.mesos;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.Subscribe;
import com.google.inject.Inject;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.stats.StatsProvider.RequestTimer;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.scheduler.events.PubsubEvent.EventSubscriber;
import org.apache.aurora.scheduler.events.PubsubEvent.TaskStatusReceived;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskStatus.Reason;
import org.apache.mesos.Protos.TaskStatus.Source;

import static java.util.Objects.requireNonNull;

/**
 * A status event listener that exports statistics about the contents of status updates.
 */
class TaskStatusStats implements EventSubscriber {

  private static final Logger LOG = Logger.getLogger(TaskStatusStats.class.getName());

  private final Clock clock;

  private final LoadingCache<Source, AtomicLong> lostSourceCounters;
  private final LoadingCache<Reason, AtomicLong> reasonCounters;

  private final LoadingCache<Source, RequestTimer> latencyTimers;

  @Inject
  TaskStatusStats(final StatsProvider statsProvider, Clock clock) {
    requireNonNull(statsProvider);
    this.clock = requireNonNull(clock);

    lostSourceCounters = CacheBuilder.newBuilder()
        .build(new CacheLoader<Source, AtomicLong>() {
          @Override
          public AtomicLong load(Source source) {
            return statsProvider.makeCounter(lostCounterName(source));
          }
        });
    reasonCounters = CacheBuilder.newBuilder()
        .build(new CacheLoader<Reason, AtomicLong>() {
          @Override
          public AtomicLong load(Reason reason) {
            return statsProvider.makeCounter(reasonCounterName(reason));
          }
        });
    latencyTimers = CacheBuilder.newBuilder()
        .build(new CacheLoader<Source, RequestTimer>() {
          @Override
          public RequestTimer load(Source source) {
            return statsProvider.makeRequestTimer(latencyTimerName(source));
          }
        });
  }

  @VisibleForTesting
  static String lostCounterName(Source source) {
    return "task_lost_" + source;
  }

  @VisibleForTesting
  static String reasonCounterName(Reason reason) {
    return "task_exit_" + reason;
  }

  @VisibleForTesting
  static String latencyTimerName(Source source) {
    return "task_delivery_delay_" + source;
  }

  @Subscribe
  public void accumulate(TaskStatusReceived event) {
    if (event.getState() == Protos.TaskState.TASK_LOST && event.getSource().isPresent()) {
      lostSourceCounters.getUnchecked(event.getSource().get()).incrementAndGet();
    }

    if (event.getReason().isPresent()) {
      reasonCounters.getUnchecked(event.getReason().get()).incrementAndGet();
    }

    if (event.getSource().isPresent() && event.getEpochTimestampMicros().isPresent()) {
      long nowMicros = clock.nowMillis() * 1000;
      // Avoid distorting stats by recording zero or negative values.  This can result if delivery
      // is faster than the clock resolution (1 ms) or there is clock skew between the systems.
      // In reality, this value is likely to be inaccurate, especially at the resolution of millis.
      if (event.getEpochTimestampMicros().get() < nowMicros) {
        latencyTimers.getUnchecked(event.getSource().get())
            .requestComplete(nowMicros - event.getEpochTimestampMicros().get());
      } else {
        LOG.fine("Not recording stats for status update with timestamp <= now");
      }
    }
  }
}
