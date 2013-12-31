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
package org.apache.aurora.scheduler.storage.mem;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.entities.ILockKey;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class MemLockStoreTest {
  private LockStore.Mutable store;

  @Before
  public void setUp() {
    store = new MemLockStore();
  }

  @Test
  public void testLocks() {
    final String role = "testRole";
    final String env = "testEnv";
    final String job1 = "testJob1";
    final String job2 = "testJob2";
    ILock lock1 = ILock.build(new Lock()
        .setKey(LockKey.job(JobKeys.from(role, env, job1).newBuilder()))
        .setToken("lock1")
        .setUser("testUser")
        .setTimestampMs(12345L));
    ILock lock2 = ILock.build(new Lock()
        .setKey(LockKey.job(JobKeys.from(role, env, job2).newBuilder()))
        .setToken("lock2")
        .setUser("testUser")
        .setTimestampMs(12345L)
        .setMessage("Test message"));

    store.saveLock(lock1);
    store.saveLock(lock2);

    assertEquals(Optional.of(lock1),
        store.fetchLock(ILockKey.build(LockKey.job(JobKeys.from(role, env, job1).newBuilder()))));
    assertEquals(Optional.of(lock2),
        store.fetchLock(ILockKey.build(LockKey.job(JobKeys.from(role, env, job2).newBuilder()))));
    assertEquals(ImmutableSet.of(lock1, lock2), store.fetchLocks());

    store.removeLock(ILockKey.build(LockKey.job(JobKeys.from(role, env, job1).newBuilder())));
    assertEquals(Optional.<ILock>absent(),
        store.fetchLock(ILockKey.build(LockKey.job(JobKeys.from(role, env, job1).newBuilder()))));

    assertEquals(Optional.of(lock2),
        store.fetchLock(ILockKey.build(LockKey.job(JobKeys.from(role, env, job2).newBuilder()))));
    assertNotNull(
        store.fetchLock(ILockKey.build(LockKey.job(JobKeys.from(role, env, job2).newBuilder())))
        .get().getMessage());
    assertEquals(ImmutableSet.of(lock2), store.fetchLocks());
  }
}
