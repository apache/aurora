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
package org.apache.aurora.scheduler.storage.db;

import java.io.IOException;
import java.util.Optional;

import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DbLockStoreTest {

  private Storage storage;

  private void assertLocks(ILock... expected) {
    assertEquals(
        ImmutableSet.<ILock>builder().add(expected).build(),
        storage.read(storeProvider -> storeProvider.getLockStore().fetchLocks()));
  }

  private Optional<ILock> getLock(ILockKey key) {
    return storage.read(storeProvider -> storeProvider.getLockStore().fetchLock(key));
  }

  private void saveLocks(ILock... locks) {
    storage.write(storeProvider -> {
      for (ILock lock : locks) {
        storeProvider.getLockStore().saveLock(lock);
      }
      return null;
    });
  }

  private void removeLocks(ILock... locks) {
    storage.write(storeProvider -> {
      for (ILock lock : locks) {
        storeProvider.getLockStore().removeLock(lock.getKey());
      }
      return null;
    });
  }

  private static ILock makeLock(JobKey key, String token) {
    return ILock.build(new Lock()
      .setKey(LockKey.job(key))
      .setToken(token)
      .setUser("testUser")
      .setMessage("Test message")
      .setTimestampMs(12345L));
  }

  @Before
  public void setUp() throws IOException {
    storage = DbUtil.createStorage();
  }

  @Test
  public void testLocks() throws Exception {
    assertLocks();

    String role = "testRole";
    String env = "testEnv";
    String job1 = "testJob1";
    String job2 = "testJob2";

    ILock lock1 = makeLock(JobKeys.from(role, env, job1).newBuilder(), "token1");
    ILock lock2 = makeLock(JobKeys.from(role, env, job2).newBuilder(), "token2");

    saveLocks(lock1, lock2);
    assertLocks(lock1, lock2);
    removeLocks(lock1);

    assertLocks(lock2);
  }

  @Test
  public void testRepeatedWrite() throws Exception {
    assertLocks();

    String role = "testRole";
    String env = "testEnv";
    String job = "testJob";

    ILock lock = makeLock(JobKeys.from(role, env, job).newBuilder(), "token1");

    saveLocks(lock);
    try {
      saveLocks(lock);
      fail("saveLock should have failed unique constraint check.");
    } catch (StorageException e) {
      // expected
    }

    assertLocks(lock);
  }

  @Test
  public void testExistingJobKey() throws Exception {
    String role = "testRole";
    String env = "testEnv";
    String job = "testJob";

    ILock lock = makeLock(JobKeys.from(role, env, job).newBuilder(), "token1");

    saveLocks(lock);
    removeLocks(lock);
    saveLocks(lock);

    assertLocks(lock);
  }

  @Test
  public void testGetLock() throws Exception {
    assertLocks();

    String role1 = "testRole1";
    String role2 = "testRole2";
    String env = "testEnv";
    String job = "testJob";

    ILock lock1 = makeLock(JobKeys.from(role1, env, job).newBuilder(), "token1");
    ILock lock2 = makeLock(JobKeys.from(role2, env, job).newBuilder(), "token2");

    assertEquals(Optional.empty(), getLock(lock1.getKey()));
    assertEquals(Optional.empty(), getLock(lock2.getKey()));

    saveLocks(StorageEntityUtil.assertFullyPopulated(lock1));
    assertEquals(Optional.of(lock1), getLock(lock1.getKey()));
    assertEquals(Optional.empty(), getLock(lock2.getKey()));
    saveLocks(lock2);
    assertEquals(Optional.of(lock1), getLock(lock1.getKey()));
    assertEquals(Optional.of(lock2), getLock(lock2.getKey()));
    removeLocks(lock1);
    assertEquals(Optional.empty(), getLock(lock1.getKey()));
    assertEquals(Optional.of(lock2), getLock(lock2.getKey()));
  }

  @Test
  public void testDeleteAllLocks() throws Exception {
    assertLocks();

    String role = "testRole";
    String env = "testEnv";
    String job1 = "testJob1";
    String job2 = "testJob2";

    ILock lock1 = makeLock(JobKeys.from(role, env, job1).newBuilder(), "token1");
    ILock lock2 = makeLock(JobKeys.from(role, env, job2).newBuilder(), "token2");

    saveLocks(lock1, lock2);
    assertLocks(lock1, lock2);

    storage.write(storeProvider -> {
      storeProvider.getLockStore().deleteLocks();
      return null;
    });

    assertLocks();
  }

  @Test
  public void testDuplicateToken() throws Exception {
    ILock lock = makeLock(JobKeys.from("role", "env", "job1").newBuilder(), "token1");
    saveLocks(lock);
    try {
      saveLocks(makeLock(JobKeys.from("role", "env", "job2").newBuilder(), "token1"));
      fail();
    } catch (StorageException e) {
      // Expected.
    }

    assertLocks(lock);
  }
}
