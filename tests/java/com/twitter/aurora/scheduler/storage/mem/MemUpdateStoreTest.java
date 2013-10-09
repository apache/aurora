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
package com.twitter.aurora.scheduler.storage.mem;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.Lock;
import com.twitter.aurora.gen.LockKey;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskUpdateConfiguration;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.storage.UpdateStore;
import com.twitter.aurora.scheduler.storage.entities.IJobKey;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class MemUpdateStoreTest {
  private static final JobUpdateConfiguration CONFIG_A = makeConfig("a");
  private static final JobUpdateConfiguration CONFIG_B = makeConfig("b");
  private static final JobUpdateConfiguration CONFIG_C = makeConfig("c");

  private static final IJobKey KEY_A = IJobKey.build(CONFIG_A.getJobKey());
  private static final IJobKey KEY_B = IJobKey.build(CONFIG_B.getJobKey());

  private UpdateStore.Mutable store;

  @Before
  public void setUp() {
    store = new MemUpdateStore();
  }

  @Test
  public void testUpdateStore() {
    assertEquals(
        Optional.<JobUpdateConfiguration>absent(),
        store.fetchJobUpdateConfig(JobKeys.from("role", "env", "job")));
    assertEquals(ImmutableSet.<JobUpdateConfiguration>of(), store.fetchUpdateConfigs("role"));
    assertEquals(ImmutableSet.<String>of(), store.fetchUpdatingRoles());

    store.saveJobUpdateConfig(CONFIG_A);
    store.saveJobUpdateConfig(CONFIG_B);
    store.saveJobUpdateConfig(CONFIG_C);
    assertEquals(Optional.of(CONFIG_A), store.fetchJobUpdateConfig(makeKey("a")));
    assertEquals(ImmutableSet.of(CONFIG_A), store.fetchUpdateConfigs("role-a"));
    assertEquals(ImmutableSet.of("role-a", "role-b", "role-c"), store.fetchUpdatingRoles());

    store.removeShardUpdateConfigs(KEY_A);
    store.removeShardUpdateConfigs(KEY_B);
    assertEquals(
        Optional.<JobUpdateConfiguration>absent(),
        store.fetchJobUpdateConfig(makeKey("a")));
    assertEquals(Optional.of(CONFIG_C), store.fetchJobUpdateConfig(makeKey("c")));
    assertEquals(ImmutableSet.of(CONFIG_C), store.fetchUpdateConfigs("role-c"));
    assertEquals(ImmutableSet.of("role-c"), store.fetchUpdatingRoles());

    store.deleteShardUpdateConfigs();
    assertEquals(
        Optional.<JobUpdateConfiguration>absent(),
        store.fetchJobUpdateConfig(makeKey("c")));
    assertEquals(ImmutableSet.<JobUpdateConfiguration>of(), store.fetchUpdateConfigs("role"));
    assertEquals(ImmutableSet.<String>of(), store.fetchUpdatingRoles());
  }

  @Test
  public void testUpdateStoreDifferentEnvironments() {
    JobUpdateConfiguration staging = makeConfig(JobKeys.from("role", "staging", "name"));
    JobUpdateConfiguration prod = makeConfig(JobKeys.from("role", "prod", "name"));

    store.saveJobUpdateConfig(staging);
    store.saveJobUpdateConfig(prod);

    assertNull(store.fetchJobUpdateConfig(
        IJobKey.build(staging.getJobKey().deepCopy().setEnvironment("devel"))).orNull());
    assertEquals(staging, store.fetchJobUpdateConfig(IJobKey.build(staging.getJobKey())).orNull());
    assertEquals(prod, store.fetchJobUpdateConfig(IJobKey.build(prod.getJobKey())).orNull());

    store.removeShardUpdateConfigs(IJobKey.build(staging.getJobKey()));
    assertNull(store.fetchJobUpdateConfig(IJobKey.build(staging.getJobKey())).orNull());
    assertEquals(prod, store.fetchJobUpdateConfig(IJobKey.build(prod.getJobKey())).orNull());
  }

  @Test
  public void testLocks() {
    final String role = "testRole";
    final String env = "testEnv";
    final String job1 = "testJob1";
    final String job2 = "testJob2";
    Lock lock1 = new Lock(
        LockKey.job(JobKeys.from(role, env, job1).newBuilder()),
        "lock1",
        "testUser",
        12345L);
    Lock lock2 = new Lock(
        LockKey.job(JobKeys.from(role, env, job2).newBuilder()),
        "lock2",
        "testUser",
        12345L);
    lock2.setMessage("Test message");

    store.saveLock(lock1);
    store.saveLock(lock2);

    assertEquals(Optional.of(lock1),
        store.fetchLock(LockKey.job(JobKeys.from(role, env, job1).newBuilder())));
    assertEquals(Optional.of(lock2),
        store.fetchLock(LockKey.job(JobKeys.from(role, env, job2).newBuilder())));
    assertEquals(ImmutableSet.of(lock1, lock2), store.fetchLocks());

    store.removeLock(LockKey.job(JobKeys.from(role, env, job1).newBuilder()));
    assertEquals(Optional.<Lock>absent(),
        store.fetchLock(LockKey.job(JobKeys.from(role, env, job1).newBuilder())));

    assertEquals(Optional.of(lock2),
        store.fetchLock(LockKey.job(JobKeys.from(role, env, job2).newBuilder())));
    assertNotNull(store.fetchLock(LockKey.job(JobKeys.from(role, env, job2).newBuilder()))
        .get().getMessage());
    assertEquals(ImmutableSet.of(lock2), store.fetchLocks());
  }

  private static IJobKey makeKey(String id) {
    return JobKeys.from("role-" + id, "env-" + id, id);
  }

  private static JobUpdateConfiguration makeConfig(String id) {
    return makeConfig(makeKey(id));
  }

  private static JobUpdateConfiguration makeConfig(IJobKey jobKey) {
    TaskConfig template = new TaskConfig()
        .setOwner(new Identity().setRole(jobKey.getRole()).setUser("user-" + jobKey.getName()))
        .setEnvironment(jobKey.getEnvironment())
        .setJobName(jobKey.getName());

    return new JobUpdateConfiguration().setJobKey(jobKey.newBuilder())
        .setConfigs(ImmutableSet.of(
            new TaskUpdateConfiguration(
                template.deepCopy().setRequestedPorts(ImmutableSet.of("old")),
                template.deepCopy().setRequestedPorts(ImmutableSet.of("new")))));
  }
}
