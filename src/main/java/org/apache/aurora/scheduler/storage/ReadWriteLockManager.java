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
package org.apache.aurora.scheduler.storage;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;

/**
 * A lock manager that wraps a ReadWriteLock and detects ill-fated attempts to upgrade
 * a read-locked thread to a write-locked thread, which would otherwise deadlock.
 */
public class ReadWriteLockManager {
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  enum LockMode {
    NONE,
    READ,
    WRITE
  }

  private static class LockState {
    private LockMode initialLockMode = LockMode.NONE;
    private int lockCount = 0;

    private boolean lockAcquired(LockMode mode) {
      boolean stateChanged = false;
      if (initialLockMode == LockMode.NONE) {
        initialLockMode = mode;
        stateChanged = true;
      }
      if (initialLockMode == mode) {
        lockCount++;
      }
      return stateChanged;
    }

    private void lockReleased(LockMode mode) {
      if (initialLockMode == mode) {
        lockCount--;
        if (lockCount == 0) {
          initialLockMode = LockMode.NONE;
        }
      }
    }
  }

  private final ThreadLocal<LockState> lockState = new ThreadLocal<LockState>() {
    @Override protected LockState initialValue() {
      return new LockState();
    }
  };

  /**
   * Blocks until this thread has acquired a read lock.
   *
   * @return {@code true} if the lock was newly-acquired, or {@code false} if this thread previously
   *         secured the write lock and has yet to release it.
   */
  public boolean readLock() {
    lock.readLock().lock();
    return lockState.get().lockAcquired(LockMode.READ);
  }

  /**
   * Releases this thread's read lock.
   */
  public void readUnlock() {
    lock.readLock().unlock();
    lockState.get().lockReleased(LockMode.READ);
  }

  /**
   * Blocks until this thread has acquired a write lock.
   *
   * @return {@code true} if the lock was newly-acquired, or {@code false} if this thread previously
   *         secured the write lock and has yet to release it.
   */
  public boolean writeLock() {
    Preconditions.checkState(lockState.get().initialLockMode != LockMode.READ,
        "A read operation may not be upgraded to a write operation.");

    lock.writeLock().lock();
    return lockState.get().lockAcquired(LockMode.WRITE);
  }

  /**
   * Releases this thread's write lock.
   */
  public void writeUnlock() {
    lock.writeLock().unlock();
    lockState.get().lockReleased(LockMode.WRITE);
  }
}
