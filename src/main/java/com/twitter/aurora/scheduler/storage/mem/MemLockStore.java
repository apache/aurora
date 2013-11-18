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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import com.twitter.aurora.scheduler.storage.LockStore;
import com.twitter.aurora.scheduler.storage.entities.ILock;
import com.twitter.aurora.scheduler.storage.entities.ILockKey;

/**
 * An in-memory lock store.
 */
class MemLockStore implements LockStore.Mutable {

  private final Map<ILockKey, ILock> locks = Maps.newConcurrentMap();

  @Override
  public void saveLock(ILock lock) {
    locks.put(lock.getKey(), lock);
  }

  @Override
  public void removeLock(ILockKey lockKey) {
    locks.remove(lockKey);
  }

  @Override
  public void deleteLocks() {
    locks.clear();
  }

  @Override
  public Set<ILock> fetchLocks() {
    return ImmutableSet.copyOf(locks.values());
  }

  @Override
  public Optional<ILock> fetchLock(ILockKey lockKey) {
    return Optional.fromNullable(locks.get(lockKey));
  }
}
