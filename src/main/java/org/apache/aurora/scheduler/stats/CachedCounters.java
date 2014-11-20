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
package org.apache.aurora.scheduler.stats;

import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.twitter.common.stats.StatsProvider;

/**
 * A cache of stats, allowing counters to be fetched and reused based on their names.
 */
public class CachedCounters {
  private final LoadingCache<String, AtomicLong> cache;

  @VisibleForTesting
  @Inject
  public CachedCounters(final StatsProvider stats) {
    cache = CacheBuilder.newBuilder().build(
        new CacheLoader<String, AtomicLong>() {
          @Override
          public AtomicLong load(String key) {
            return stats.makeCounter(key);
          }
        }
    );
  }

  public AtomicLong get(String name) {
    return cache.getUnchecked(name);
  }
}
