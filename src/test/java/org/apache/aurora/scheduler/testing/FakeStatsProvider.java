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
package org.apache.aurora.scheduler.testing;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Supplier;
import com.google.common.collect.Maps;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.StatsProvider;

/**
 * A fake stats provider for use in testing.
 */
public class FakeStatsProvider implements StatsProvider {
  private final Map<String, Supplier<? extends Number>> stats = Maps.newHashMap();

  /**
   * Gets the current value of a stat.
   *
   * @param statName Name of the stat to fetch.
   * @return Current stat value.
   */
  public Number getValue(String statName) {
    return stats.get(statName).get();
  }

  @Override
  public AtomicLong makeCounter(String name) {
    final AtomicLong counter = new AtomicLong();
    stats.put(name, new Supplier<Long>() {
      @Override
      public Long get() {
        return counter.get();
      }
    });
    return counter;
  }

  @Override
  public <T extends Number> Stat<T> makeGauge(final String name, final Supplier<T> gauge) {
    stats.put(name, gauge);

    return new Stat<T>() {
      @Override
      public String getName() {
        return name;
      }

      @Override
      public T read() {
        return gauge.get();
      }
    };
  }

  @Override
  public StatsProvider untracked() {
    return this;
  }

  @Override
  public RequestTimer makeRequestTimer(String name) {
    throw new UnsupportedOperationException();
  }
}
