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
package org.apache.aurora.scheduler.preemptor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;

import static java.util.Objects.requireNonNull;

/**
 * A bi-directional cache of items. Entries are purged from cache after
 * {@link BiCacheSettings#expireAfter}.
 *
 * @param <K> Key type.
 * @param <V> Value type.
 */
public class BiCache<K, V> {

  public static class BiCacheSettings {
    private final Amount<Long, Time> expireAfter;
    private final String cacheName;

    public BiCacheSettings(Amount<Long, Time> expireAfter, String cacheName) {
      this.expireAfter = requireNonNull(expireAfter);
      this.cacheName = requireNonNull(cacheName);
    }
  }

  private final Cache<K, V> cache;
  private final Multimap<V, K> inverse = HashMultimap.create();
  private final AtomicLong removalCounter;
  private final AtomicLong expirationCounter;
  private final AtomicLong explictRemovalCounter;

  @Inject
  public BiCache(
      StatsProvider statsProvider,
      BiCacheSettings settings,
      final Clock clock) {

    requireNonNull(clock);
    this.cache = CacheBuilder.newBuilder()
        .expireAfterWrite(settings.expireAfter.as(Time.MINUTES), TimeUnit.MINUTES)
        .ticker(new Ticker() {
          @Override
          public long read() {
            return clock.nowNanos();
          }
        })
        .removalListener(new RemovalListener<K, V>() {
          @Override
          public void onRemoval(RemovalNotification<K, V> notification) {
            removalCounter.getAndIncrement();
            if (notification.wasEvicted()) {
              expirationCounter.incrementAndGet();
            }
            inverse.remove(notification.getValue(), notification.getKey());
          }
        })
        .build();

    statsProvider.makeGauge(settings.cacheName + "_cache_size", cache::size);
    removalCounter = statsProvider.makeCounter(settings.cacheName + "_cache_removals");
    expirationCounter = statsProvider.makeCounter(
        settings.cacheName + "_cache_expiration_removals");
    explictRemovalCounter = statsProvider.makeCounter(
        settings.cacheName + "_cache_explicit_removals");
  }

  /**
   * Puts a new key/value pair.
   *
   * @param key Key to add.
   * @param value Value to add.
   */
  public synchronized void put(K key, V value) {
    requireNonNull(key);
    requireNonNull(value);
    cache.put(key, value);
    inverse.put(value, key);
  }

  /**
   * Gets a cached value by key.
   *
   * @param key Key to get value for.
   * @return Optional of value.
   */
  public synchronized Optional<V> get(K key) {
    return Optional.fromNullable(cache.getIfPresent(key));
  }

  /**
   * Gets a set of keys for a given value.
   *
   * @param value Value to get all keys for.
   * @return An {@link Iterable} of keys or empty if value does not exist.
   */
  public synchronized Set<K> getByValue(V value) {
    // Cache items are lazily removed by routine maintenance operations during get/write access.
    // Forcing cleanup here to ensure proper data integrity.
    cache.cleanUp();
    return ImmutableSet.copyOf(inverse.get(value));
  }

  /**
   * Removes a key/value pair from cache.
   *
   * @param key Key to remove.
   * @param value Value to remove.
   */
  public synchronized void remove(K key, V value) {
    explictRemovalCounter.getAndIncrement();
    inverse.remove(value, key);
    cache.invalidate(key);
  }

  /**
   * Returns an immutable copy of entries stored in this cache.
   *
   * @return Immutable map of cache entries.
   */
  public synchronized Map<K, V> asMap() {
    return ImmutableMap.copyOf(cache.asMap());
  }
}
