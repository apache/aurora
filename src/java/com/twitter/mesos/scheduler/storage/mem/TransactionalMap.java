package com.twitter.mesos.scheduler.storage.mem;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.mesos.scheduler.storage.Transactional;

/**
 * A map that wraps another map implementation, adding the ability to perform transactions.
 * A transaction effectively begins the first time the map is modified.  At any point the map may
 * be committed or rolled back, which will maintain or revert any changes since the last commit
 * or rollback.
 * <p>
 * Entries may only be modified by invoking {@link Map} interface methods, it does not support
 * modification by accessing the {@link #keySet()}, {@link #values()}, or {@link #entrySet()}.
 * Attempting to do so will result in a {@link UnsupportedOperationException}.
 * <p>
 * This class is not thread safe.  It could be trivially made thread-safe, but mutual exclusion
 * on mutation access would need to be handled externally if cross-thread visibility of uncommitted
 * modifications is not acceptable.
 *
 * TODO(William Farner): Rename this to RevertableMap to more closely match the functionality.
 */
class TransactionalMap<K, V> implements Map<K, V>, Transactional {

  private final Map<K, V> wrapped;
  private final Rollback<K, V> rollback = new Rollback<K, V>();

  TransactionalMap(Map<K, V> wrapped) {
    this.wrapped = Preconditions.checkNotNull(wrapped);
  }

  static <K, V> TransactionalMap<K, V> wrap(Map<K, V> map) {
    return new TransactionalMap<K, V>(map);
  }

  @Override
  public void commit() {
    rollback.consumeAll(Closures.<Map.Entry<K, Optional<V>>>noop());
  }

  @Override
  public void rollback() {
    rollback.consumeAll(new Closure<Map.Entry<K, Optional<V>>>() {
      @Override public void execute(Map.Entry<K, Optional<V>> revert) {
        if (revert.getValue().isPresent()) {
          wrapped.put(revert.getKey(), revert.getValue().get());
        } else {
          wrapped.remove(revert.getKey());
        }
      }
    });
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public boolean isEmpty() {
    return wrapped.isEmpty();
  }

  @Override
  public boolean containsKey(Object o) {
    return wrapped.containsKey(o);
  }

  @Override
  public boolean containsValue(Object o) {
    return wrapped.containsValue(o);
  }

  @Override
  public V get(Object o) {
    return wrapped.get(o);
  }

  @Override
  public V put(K key, V value) {
    @Nullable V displaced = wrapped.put(key, value);
    if (displaced != null) {
      rollback.entryDisplaced(key, displaced);
    } else {
      rollback.entryAdded(key);
    }
    return displaced;
  }

  @Override
  public V remove(Object o) {
    @Nullable V removed = wrapped.remove(o);
    if (removed != null) {
      @SuppressWarnings("unchecked")
      K key = (K) o;
      rollback.entryDisplaced(key, removed);
    }

    return removed;
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> map) {
    for (Map.Entry<? extends K, ? extends V> entry : map.entrySet()) {
      put(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void clear() {
    for (Map.Entry<K, V> entry : wrapped.entrySet()) {
      rollback.entryDisplaced(entry.getKey(), entry.getValue());
    }
    wrapped.clear();
  }

  @Override
  public Set<K> keySet() {
    return Collections.unmodifiableSet(wrapped.keySet());
  }

  @Override
  public Collection<V> values() {
    return Collections.unmodifiableCollection(wrapped.values());
  }

  @Override
  public Set<Entry<K, V>> entrySet() {
    return Collections.unmodifiableSet(wrapped.entrySet());
  }

  private static class Rollback<K, V> {
    final Map<K, Optional<V>> rollbackState = Maps.newHashMap();

    void entryDisplaced(K key, V oldValue) {
      if (!rollbackState.containsKey(key)) {
        rollbackState.put(key, Optional.of(oldValue));
      }
    }

    void entryAdded(K key) {
      if (!rollbackState.containsKey(key)) {
        rollbackState.put(key, Optional.<V>absent());
      }
    }

    void consumeAll(Closure<Entry<K, Optional<V>>> sink) {
      for (Map.Entry<K, Optional<V>> revert : rollbackState.entrySet()) {
        sink.execute(revert);
      }
      rollbackState.clear();
    }
  }
}
