package com.twitter.mesos.scheduler.storage;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Preconditions;

/**
 * A lock manager that wraps a ReadWriteLock and detects ill-fated attempts to upgrade
 * a read-locked thread to a write-locked thread, which would otherwise deadlock.
 */
public class LockManager {
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  enum LockMode {
    NONE,
    READ,
    WRITE
  }

  private static class LockState {
    LockMode initialLockMode = LockMode.NONE;
    int lockCount = 0;

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
   */
  public void readLock() {
    lock.readLock().lock();
    lockState.get().lockAcquired(LockMode.READ);
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
        "A read transaction may not be upgraded to a write transaction.");

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
