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
package com.twitter.aurora.scheduler.state;

import java.util.Date;

import com.google.common.base.Optional;
import com.google.inject.Inject;

import com.twitter.aurora.gen.Lock;
import com.twitter.aurora.scheduler.storage.Storage;
import com.twitter.aurora.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.MutateWork;
import com.twitter.aurora.scheduler.storage.Storage.StoreProvider;
import com.twitter.aurora.scheduler.storage.Storage.Work;
import com.twitter.aurora.scheduler.storage.UpdateStore;
import com.twitter.aurora.scheduler.storage.entities.ILock;
import com.twitter.aurora.scheduler.storage.entities.ILockKey;
import com.twitter.common.util.Clock;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Implements lock-related primitives required to provide mutual exclusion guarantees
 * to the critical Scheduler state-mutating operations.
 */
class LockManagerImpl implements LockManager {
  private final Storage storage;
  private final Clock clock;
  private final UUIDGenerator tokenGenerator;

  @Inject
  LockManagerImpl(Storage storage, Clock clock, UUIDGenerator tokenGenerator) {
    this.storage = checkNotNull(storage);
    this.clock = checkNotNull(clock);
    this.tokenGenerator = checkNotNull(tokenGenerator);
  }

  @Override
  public ILock acquireLock(final ILockKey lockKey, final String user) throws LockException {
    return storage.write(new MutateWork<ILock, LockException>() {
      @Override public ILock apply(Storage.MutableStoreProvider storeProvider)
          throws LockException {

        UpdateStore.Mutable updateStore = storeProvider.getUpdateStore();
        Optional<ILock> existingLock = updateStore.fetchLock(lockKey);

        if (existingLock.isPresent()) {
          throw new LockException(String.format(
              "Operation for: %s is already in progress. Started at: %s. Current owner: %s.",
              lockKey,
              new Date(existingLock.get().getTimestampMs()).toString(),
              existingLock.get().getUser()));
        }

        ILock lock = ILock.build(new Lock()
            .setKey(lockKey.newBuilder())
            .setToken(tokenGenerator.createNew().toString())
            .setTimestampMs(clock.nowMillis())
            .setUser(user));

        updateStore.saveLock(lock);
        return lock;
      }
    });
  }

  @Override
  public void releaseLock(final ILock lock) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getUpdateStore().removeLock(lock.getKey());
      }
    });
  }

  @Override
  public synchronized void validateLock(final ILock lock) throws LockException {
    Optional<ILock> stored = storage.consistentRead(new Work.Quiet<Optional<ILock>>() {
      @Override public Optional<ILock> apply(StoreProvider storeProvider) {
        return storeProvider.getUpdateStore().fetchLock(lock.getKey());
      }
    });

    if (!stored.isPresent()) {
      throw new LockException("No operation is in progress for: " + lock.getKey());
    }

    if (!stored.get().equals(lock)) {
      throw new LockException(String.format(
          "Unable to finish operation for: %s. Use override/cancel option.",
          lock.getKey()));
    }
  }
}
