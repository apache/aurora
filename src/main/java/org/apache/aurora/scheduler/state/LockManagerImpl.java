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
package org.apache.aurora.scheduler.state;

import java.util.Date;
import java.util.Optional;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;

import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey._Fields;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;

import static java.util.Objects.requireNonNull;

/**
 * Implements lock-related primitives required to provide mutual exclusion guarantees
 * to the critical Scheduler state-mutating operations.
 */
@VisibleForTesting
public class LockManagerImpl implements LockManager {
  private final Storage storage;
  private final Clock clock;
  private final UUIDGenerator tokenGenerator;

  @Inject
  LockManagerImpl(Storage storage, Clock clock, UUIDGenerator tokenGenerator) {
    this.storage = requireNonNull(storage);
    this.clock = requireNonNull(clock);
    this.tokenGenerator = requireNonNull(tokenGenerator);
  }

  @Override
  public ILock acquireLock(final ILockKey lockKey, final String user) throws LockException {
    return storage.write(storeProvider -> {

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
    });
  }

  @Override
  public void releaseLock(final ILock lock) {
    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getLockStore().removeLock(lock.getKey()));
  }

  @Override
  public void assertNotLocked(final ILockKey context) throws LockException {
    Optional<ILock> stored = storage.read(
        storeProvider -> storeProvider.getLockStore().fetchLock(context));
    if (stored.isPresent()) {
      throw new LockException(String.format(
          "Unable to perform operation for %s due to active lock held",
          formatLockKey(context)));
    }
  }

  @Override
  public Iterable<ILock> getLocks() {
    return storage.read(storeProvider -> storeProvider.getLockStore().fetchLocks());
  }

  private static String formatLockKey(ILockKey lockKey) {
    return lockKey.getSetField() == _Fields.JOB
        ? JobKeys.canonicalString(lockKey.getJob())
        : "Unknown lock key type: " + lockKey.getSetField();
  }
}
