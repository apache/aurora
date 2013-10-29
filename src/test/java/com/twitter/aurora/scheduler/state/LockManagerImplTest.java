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

import java.util.UUID;

import com.google.common.base.Optional;

import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.Lock;
import com.twitter.aurora.gen.LockKey;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.state.LockManager.LockException;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;
import com.twitter.aurora.scheduler.storage.entities.ILock;
import com.twitter.aurora.scheduler.storage.entities.ILockKey;
import com.twitter.aurora.scheduler.storage.mem.MemStorage;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.testing.easymock.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;

import static org.junit.Assert.assertEquals;

import static com.twitter.aurora.gen.Constants.DEFAULT_ENVIRONMENT;

public class LockManagerImplTest extends EasyMockTest {
  private static final String USER = "jim-user";
  private static final Identity JIM = new Identity("jim", USER);
  private static final String MY_JOB = "myJob";
  private static final IJobKey JOB_KEY = JobKeys.from(JIM.getRole(), DEFAULT_ENVIRONMENT, MY_JOB);
  private static final ILockKey LOCK_KEY = ILockKey.build(LockKey.job(JOB_KEY.newBuilder()));
  private static final UUID TOKEN = UUID.fromString("79d6d790-3212-11e3-aa6e-0800200c9a66");

  private LockManager lockManager;
  private long timestampMs;

  @Before
  public void setUp() throws Exception {
    FakeClock clock = new FakeClock();
    clock.advance(Amount.of(12345L, Time.SECONDS));
    timestampMs = clock.nowMillis();

    UUIDGenerator tokenGenerator = createMock(UUIDGenerator.class);
    EasyMock.expect(tokenGenerator.createNew()).andReturn(TOKEN).anyTimes();

    lockManager = new LockManagerImpl(MemStorage.newEmptyStorage(), clock, tokenGenerator);
    control.replay();
  }

  @Test
  public void testAcquireLock() throws Exception {
    ILock expected = ILock.build(new Lock()
        .setKey(LOCK_KEY.newBuilder())
        .setToken(TOKEN.toString())
        .setTimestampMs(timestampMs)
        .setUser(USER));

    ILock actual = lockManager.acquireLock(expected.getKey(), USER);
    assertEquals(expected, actual);
  }

  @Test(expected = LockException.class)
  public void testAcquireLockInProgress() throws Exception {
    lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.acquireLock(LOCK_KEY, USER);
  }

  @Test
  public void testReleaseLock() throws Exception {
    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.releaseLock(lock);

    // Should be able to lock again after releasing.
    lockManager.acquireLock(LOCK_KEY, USER);
  }

  @Test
  public void testValidateLockStoredEqualHeld() throws Exception {
    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(lock));
  }

  @Test
  public void testValidateLockNotStoredNotHeld() throws Exception {
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
  }

  @Test(expected = LockException.class)
  public void testValidateLockStoredNotEqualHeld() throws Exception {
    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    lock = ILock.build(lock.newBuilder().setUser("bob"));
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(lock));
  }

  @Test(expected = LockException.class)
  public void testValidateLockStoredNotEqualHeldWithHeldNull() throws Exception {
    lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.validateIfLocked(LOCK_KEY, Optional.<ILock>absent());
  }

  @Test(expected = LockException.class)
  public void testValidateLockNotStoredHeld() throws Exception {
    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    ILockKey key = ILockKey.build(LockKey.job(JobKeys.from("r", "e", "n").newBuilder()));
    lockManager.validateIfLocked(key, Optional.of(lock));
  }
}
