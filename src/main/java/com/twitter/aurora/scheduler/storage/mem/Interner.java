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
package com.twitter.aurora.scheduler.storage.mem;

import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An interning pool that can be used to retrieve the canonical instances of objects, while
 * maintaining a reference count to the canonical instances.
 *
 * @param <T> The interned object type.
 * @param <A> The type used for maintaining associations.
 */
class Interner<T, A> {

  private final Map<T, InternEntry<A, T>> pool = Maps.newHashMap();

  /**
   * Retrieves the canonical instance of {@code t} and maintains {@code association} with the
   * interned value.  If {@code t} was not previously interned, the provided instance is stored.
   *
   * @param t The object to intern, or get the previously-interned value for.
   * @param association A value to associate with {@code t}.
   * @return The interned value, which may be reference-equivalent to {@code t}.
   */
  synchronized T addAssociation(T t, A association) {
    InternEntry<A, T> entry = pool.get(t);
    if (entry == null) {
      entry = new InternEntry<A, T>(t, association);
      pool.put(t, entry);
    } else {
      entry.associations.add(association);
    }
    return entry.interned;
  }

  /**
   * Removes an association with an interned value, effectively decrementing the reference count.
   *
   * @param t The interned value that {@code association} was associated with.
   * @param association The association to remove.
   */
  synchronized void removeAssociation(T t, A association) {
    InternEntry<A, T> entry = pool.get(t);
    if (entry != null) {
      entry.associations.remove(association);
      if (entry.associations.isEmpty()) {
        pool.remove(t);
      }
    }
  }

  /**
   * Removes all interned values and associations.
   */
  synchronized void clear() {
    pool.clear();
  }

  @VisibleForTesting
  synchronized boolean isInterned(T t) {
    return pool.containsKey(t);
  }

  @VisibleForTesting
  synchronized Set<A> getAssociations(T t) {
    return ImmutableSet.copyOf(pool.get(t).associations);
  }

  private static class InternEntry<A, T> {
    private final T interned;
    private final Set<A> associations = Sets.newHashSet();

    InternEntry(T interned, A initialAssociation) {
      this.interned = interned;
      associations.add(initialAssociation);
    }
  }
}
