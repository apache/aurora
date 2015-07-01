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
package org.apache.aurora.benchmark.fakes;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.base.Supplier;
import com.twitter.common.stats.Stat;
import com.twitter.common.stats.StatsProvider;

public class FakeStatsProvider implements StatsProvider {
  @Override
  public AtomicLong makeCounter(String name) {
    return new AtomicLong();
  }

  @Override
  public <T extends Number> Stat<T> makeGauge(final String name, final Supplier<T> gauge) {
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
    return new RequestTimer() {
      @Override
      public void requestComplete(long latencyMicros) {
        // no-op
      }

      @Override
      public void incErrors() {
        // no-op
      }

      @Override
      public void incReconnects() {
        // no-op
      }

      @Override
      public void incTimeouts() {
        // no-op
      }
    };
  }
}
