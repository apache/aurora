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

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.state.LockManager.LockException;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.testing.StorageTestUtil;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.aurora.scheduler.storage.Storage.Work;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LockManagerImplTest extends EasyMockTest {
  private static final String USER = "jim-user";
  private static final String MY_JOB = "myJob";
  private static final IJobKey JOB_KEY = JobKeys.from("jim", "devel", MY_JOB);
  private static final ILockKey LOCK_KEY = ILockKey.build(LockKey.job(JOB_KEY.newBuilder()));
  private static final UUID TOKEN = UUID.fromString("79d6d790-3212-11e3-aa6e-0800200c9a66");

  private FakeClock clock;
  private UUIDGenerator tokenGenerator;
  private LockManager lockManager;
  private long timestampMs;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    clock = new FakeClock();
    clock.advance(Amount.of(12345L, Time.SECONDS));
    timestampMs = clock.nowMillis();

    tokenGenerator = createMock(UUIDGenerator.class);
    expect(tokenGenerator.createNew()).andReturn(TOKEN).anyTimes();

    lockManager = new LockManagerImpl(DbUtil.createStorage(), clock, tokenGenerator);
  }

  @Test
  public void testAcquireLock() throws Exception {
    control.replay();

    ILock expected = ILock.build(new Lock()
        .setKey(LOCK_KEY.newBuilder())
        .setToken(TOKEN.toString())
        .setTimestampMs(timestampMs)
        .setUser(USER));

    ILock actual = lockManager.acquireLock(expected.getKey(), USER);
    assertEquals(expected, actual);
  }

  @Test
  public void testAcquireLockInProgress() throws Exception {
    control.replay();

    expectLockException(JOB_KEY);
    lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.acquireLock(LOCK_KEY, USER);
  }

  @Test
  public void testReleaseLock() throws Exception {
    control.replay();

    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.releaseLock(lock);

    // Should be able to lock again after releasing.
    lockManager.acquireLock(LOCK_KEY, USER);
  }

  @Test
  public void testValidateLockStoredEqualHeld() throws Exception {
    control.replay();

    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(lock));
  }

  @Test
  public void testValidateLockNotStoredNotHeld() throws Exception {
    control.replay();

    lockManager.validateIfLocked(LOCK_KEY, Optional.empty());
  }

  @Test
  public void testValidateLockStoredNotEqualHeld() throws Exception {
    control.replay();

    expectLockException(JOB_KEY);
    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    lock = ILock.build(lock.newBuilder().setUser("bob"));
    lockManager.validateIfLocked(LOCK_KEY, Optional.of(lock));
  }

  @Test
  public void testValidateLockStoredNotEqualHeldWithHeldNull() throws Exception {
    control.replay();

    expectLockException(JOB_KEY);
    lockManager.acquireLock(LOCK_KEY, USER);
    lockManager.validateIfLocked(LOCK_KEY, Optional.empty());
  }

  @Test
  public void testValidateLockNotStoredHeld() throws Exception {
    control.replay();

    IJobKey jobKey = JobKeys.from("r", "e", "n");
    expectLockException(jobKey);
    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    ILockKey key = ILockKey.build(LockKey.job(jobKey.newBuilder()));
    lockManager.validateIfLocked(key, Optional.of(lock));
  }

  @Test
  public void testGetLocks() throws Exception {
    control.replay();

    ILock lock = lockManager.acquireLock(LOCK_KEY, USER);
    assertEquals(lock, Iterables.getOnlyElement(lockManager.getLocks()));
  }

  // Test for regression of AURORA-702.
  @Test
  public void testNoDeadlock() throws Exception {
    final StorageTestUtil storageUtil = new StorageTestUtil(this);

    expect(storageUtil.storeProvider.getLockStore()).andReturn(storageUtil.lockStore).atLeastOnce();
    expect(storageUtil.lockStore.fetchLock(LOCK_KEY))
        .andReturn(Optional.empty())
        .atLeastOnce();

    final CountDownLatch reads = new CountDownLatch(2);
    EasyMock.makeThreadSafe(storageUtil.storage, false);
    expect(storageUtil.storage.read(EasyMock.anyObject()))
        .andAnswer(() -> {
          @SuppressWarnings("unchecked")
          Work<?, ?> work = (Work<?, ?>) EasyMock.getCurrentArguments()[0];
          Object result = work.apply(storageUtil.storeProvider);
          reads.countDown();
          reads.await();
          return result;
        }).atLeastOnce();

    lockManager = new LockManagerImpl(storageUtil.storage, clock, tokenGenerator);

    control.replay();

    new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("LockRead-%s")
        .build()
        .newThread(() -> {
          try {
            lockManager.validateIfLocked(LOCK_KEY, Optional.empty());
          } catch (LockException e) {
            throw Throwables.propagate(e);
          }
        })
        .start();

    lockManager.validateIfLocked(LOCK_KEY, Optional.empty());
  }

  private void expectLockException(IJobKey key) {
    expectedException.expect(LockException.class);
    expectedException.expectMessage(JobKeys.canonicalString(key));
  }
}
