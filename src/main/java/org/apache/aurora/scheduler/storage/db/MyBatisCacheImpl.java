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
package org.apache.aurora.scheduler.storage.db;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.primitives.Ints;

import org.apache.aurora.common.stats.StatImpl;
import org.apache.aurora.common.stats.Stats;
import org.apache.ibatis.cache.Cache;

import static java.util.Objects.requireNonNull;

public class MyBatisCacheImpl implements Cache {
  private com.google.common.cache.Cache<Object, Object> delegate;
  private final String id;
  private Integer size;
  private final AtomicInteger clearCount = new AtomicInteger(0);

  public MyBatisCacheImpl(String id) {
    this.id = requireNonNull(id);
  }

  public void setSize(Integer size) {
    this.size = requireNonNull(size);
    initCache();
  }

  private void initCache() {
    Preconditions.checkState(delegate == null);
    requireNonNull(size);

    delegate = CacheBuilder.newBuilder()
        .maximumSize(size)
        .recordStats()
        .softValues()
        .build();
    initStats();
  }

  private void initStats() {
    makeStat("request_count", () -> delegate.stats().requestCount());
    makeStat("hit_count", () ->  delegate.stats().hitCount());
    makeStat("hit_rate",  () -> delegate.stats().hitRate());
    makeStat("miss_count", () -> delegate.stats().missCount());
    makeStat("miss_rate", () -> delegate.stats().missRate());
    makeStat("eviction_count", () -> delegate.stats().evictionCount());
    makeStat("size", () -> delegate.size());
    makeStat("clear_count", clearCount::get);
  }

  private <T extends Number> void makeStat(String name, Supplier<T> supplier) {
    String prefix = "db_storage_mybatis_cache_" + id + "_";
    Stats.export(new StatImpl<Number>(prefix + name) {
      @Override
      public Number read() {
        return supplier.get();
      }
    });
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void putObject(Object key, Object value) {
    if (key == null || value == null) {
      return;
    }
    delegate.put(key, value);
  }

  @Override
  public Object getObject(Object key) {
    return delegate.getIfPresent(key);
  }

  @Override
  public Object removeObject(Object key) {
    delegate.invalidate(key);
    // MyBatis says the return value is not used.
    return null;
  }

  @Override
  public void clear() {
    delegate.invalidateAll();
    clearCount.incrementAndGet();
  }

  @Override
  public int getSize() {
    return Ints.saturatedCast(delegate.size());
  }

  @Override
  public ReadWriteLock getReadWriteLock() {
    // MyBatis says this value is no longer used.
    return null;
  }
}
