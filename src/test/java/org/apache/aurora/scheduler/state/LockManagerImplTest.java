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

import java.util.UUID;

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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LockManagerImplTest extends EasyMockTest {
  private static final String USER = "jim-user";
  private static final String MY_JOB = "myJob";
  private static final IJobKey JOB_KEY = JobKeys.from("jim", "devel", MY_JOB);
  private static final UUID TOKEN = UUID.fromString("79d6d790-3212-11e3-aa6e-0800200c9a66");

  private LockManager lockManager;
  private long timestampMs;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setUp() throws Exception {
    FakeClock clock = new FakeClock();
    clock.advance(Amount.of(12345L, Time.SECONDS));
    timestampMs = clock.nowMillis();

    UUIDGenerator tokenGenerator = createMock(UUIDGenerator.class);
    expect(tokenGenerator.createNew()).andReturn(TOKEN).anyTimes();

    lockManager = new LockManagerImpl(DbUtil.createStorage(), clock, tokenGenerator);
  }

  @Test
  public void testAcquireLock() throws Exception {
    control.replay();

    ILock expected = ILock.build(new Lock()
        .setKey(LockKey.job(JOB_KEY.newBuilder()))
        .setToken(TOKEN.toString())
        .setTimestampMs(timestampMs)
        .setUser(USER));

    ILock actual = lockManager.acquireLock(JOB_KEY, USER);
    assertEquals(expected, actual);
  }

  @Test
  public void testAcquireLockInProgress() throws Exception {
    control.replay();

    expectLockException(JOB_KEY);
    lockManager.acquireLock(JOB_KEY, USER);
    lockManager.acquireLock(JOB_KEY, USER);
  }

  @Test
  public void testReleaseLock() throws Exception {
    control.replay();

    lockManager.acquireLock(JOB_KEY, USER);
    lockManager.releaseLock(JOB_KEY);

    // Should be able to lock again after releasing.
    lockManager.acquireLock(JOB_KEY, USER);
  }

  private void expectLockException(IJobKey key) {
    expectedException.expect(LockException.class);
    expectedException.expectMessage(JobKeys.canonicalString(key));
  }
}
