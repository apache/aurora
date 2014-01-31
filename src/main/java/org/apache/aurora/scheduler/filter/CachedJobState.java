/**
 * Copyright 2014 Apache Software Foundation
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
package org.apache.aurora.scheduler.filter;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.scheduler.storage.entities.IScheduledTask;

/**
 * A temporary view of a job's state.  Once constructed, instances of this class should be discarded
 * once the job state may change (e.g. after exiting a write transaction).  This is intended to
 * capture job state once and avoid redundant queries.
 * <p>
 * Note that while the state injected into this class is lazy (to allow for queries to happen only
 * on-demand), calling {@link #equals(Object)} and {@link #hashCode()} rely on the result of
 * {@link #getActiveTasks()}, thus invoking the {@link Supplier}.
 * <p>
 * TODO(wfarner): Take this caching one step further and don't store tasks here, but instead store
 * pre-computed aggregates of host attributes.  For example, translate each IScheduledTask into
 * the HostAttributes of the machine it resides on, and count the number of times each attribute is
 * seen.  This could be represented in a <pre>Map&lt;String, Multiset&lt;String&gt;&gt;</pre>, where
 * the outer key is the attribute name, and the inner key is attribute values.  So, for a job with
 * two tasks on the same rack but different hosts, you could have this aggregate:
 * <pre>
 * { "host": {"hostA": 1, "hostB": 1},
 *   "rack": {"rack1": 2}
 * }
 * </pre>
 */
public class CachedJobState {

  private final Supplier<ImmutableSet<IScheduledTask>> activeTaskSupplier;

  public CachedJobState(Supplier<ImmutableSet<IScheduledTask>> activeTaskSupplier) {
    this.activeTaskSupplier = Preconditions.checkNotNull(activeTaskSupplier);
  }

  public ImmutableSet<IScheduledTask> getActiveTasks() {
    return activeTaskSupplier.get();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof CachedJobState)) {
      return false;
    }

    CachedJobState other = (CachedJobState) o;
    return getActiveTasks().equals(other.getActiveTasks());
  }

  @Override
  public int hashCode() {
    return getActiveTasks().hashCode();
  }
}
