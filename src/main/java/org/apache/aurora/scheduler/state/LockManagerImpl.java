/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.state;

import java.util.Date;

import javax.inject.Inject;

import com.google.common.base.Optional;

import com.twitter.common.util.Clock;

import org.apache.aurora.gen.Lock;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;

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

        LockStore.Mutable lockStore = storeProvider.getLockStore();
        Optional<ILock> existingLock = lockStore.fetchLock(lockKey);

        if (existingLock.isPresent()) {
          throw new LockException(String.format(
              "Operation for: %s is already in progress. Started at: %s. Current owner: %s.",
              formatLockKey(lockKey),
              new Date(existingLock.get().getTimestampMs()).toString(),
              existingLock.get().getUser()));
        }

        ILock lock = ILock.build(new Lock()
            .setKey(lockKey.newBuilder())
            .setToken(tokenGenerator.createNew().toString())
            .setTimestampMs(clock.nowMillis())
            .setUser(user));

        lockStore.saveLock(lock);
        return lock;
      }
    });
  }

  @Override
  public void releaseLock(final ILock lock) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getLockStore().removeLock(lock.getKey());
      }
    });
  }

  @Override
  public synchronized void validateIfLocked(final ILockKey context, Optional<ILock> heldLock)
      throws LockException {

    Optional<ILock> stored = storage.consistentRead(new Work.Quiet<Optional<ILock>>() {
      @Override public Optional<ILock> apply(StoreProvider storeProvider) {
        return storeProvider.getLockStore().fetchLock(context);
      }
    });

    // The implementation below assumes the following use cases:
    // +-----------+-----------------+----------+
    // |   eq      |     held        | not held |
    // +-----------+-----------------+----------+
    // |stored     |(stored == held)?| invalid  |
    // +-----------+-----------------+----------+
    // |not stored |    invalid      |  valid   |
    // +-----------+-----------------+----------+
    if (!stored.equals(heldLock)) {
      if (stored.isPresent()) {
        throw new LockException(String.format(
            "Unable to perform operation for: %s. Use override/cancel option.",
            formatLockKey(context)));
      } else if (heldLock.isPresent()) {
        throw new LockException(
            String.format("Invalid operation context: %s", formatLockKey(context)));
      }
    }
  }

  private static String formatLockKey(ILockKey lockKey) {
    switch (lockKey.getSetField()) {
      case JOB:
        return JobKeys.toPath(lockKey.getJob());
      default:
        return "Unknown lock key type: " + lockKey.getSetField();
    }
  }
}
