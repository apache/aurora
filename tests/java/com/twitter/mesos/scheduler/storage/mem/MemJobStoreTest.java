package com.twitter.mesos.scheduler.storage.mem;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.scheduler.base.JobKeys;
import com.twitter.mesos.scheduler.storage.JobStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MemJobStoreTest {

  private static final String MANAGER_1 = "manager1";
  private static final String MANAGER_2 = "manager2";

  private static final JobConfiguration JOB_A = makeJob("a");
  private static final JobConfiguration JOB_B = makeJob("b");

  private JobStore.Mutable store;

  @Before
  public void setUp() {
    store = new MemJobStore();
  }

  @Test
  public void testJobStore() {
    assertNull(store.fetchJob(MANAGER_1, "a"));
    assertEquals(ImmutableSet.<JobConfiguration>of(), store.fetchJobs(MANAGER_1));
    assertEquals(ImmutableSet.<String>of(), store.fetchManagerIds());

    store.saveAcceptedJob(MANAGER_1, JOB_A);
    assertEquals(JOB_A, store.fetchJob(MANAGER_1, "role-a/a"));
    assertEquals(ImmutableSet.of(JOB_A), store.fetchJobs(MANAGER_1));

    store.saveAcceptedJob(MANAGER_1, JOB_B);
    assertEquals(JOB_B, store.fetchJob(MANAGER_1, "role-b/b"));
    assertEquals(ImmutableSet.of(JOB_A, JOB_B), store.fetchJobs(MANAGER_1));
    assertEquals(ImmutableSet.of(MANAGER_1), store.fetchManagerIds());

    store.saveAcceptedJob(MANAGER_2, JOB_B);
    assertEquals(JOB_B, store.fetchJob(MANAGER_1, "role-b/b"));
    assertEquals(ImmutableSet.of(JOB_B), store.fetchJobs(MANAGER_2));
    assertEquals(ImmutableSet.of(MANAGER_1, MANAGER_2), store.fetchManagerIds());

    store.removeJob("role-b/b");
    assertEquals(ImmutableSet.of(JOB_A), store.fetchJobs(MANAGER_1));
    assertEquals(ImmutableSet.<JobConfiguration>of(), store.fetchJobs(MANAGER_2));

    store.deleteJobs();
    assertEquals(ImmutableSet.<JobConfiguration>of(), store.fetchJobs(MANAGER_1));
    assertEquals(ImmutableSet.<JobConfiguration>of(), store.fetchJobs(MANAGER_2));
  }

  private static JobConfiguration makeJob(String name) {
    return new JobConfiguration()
        .setName(name)
        .setOwner(new Identity("role-" + name, "user-" + name))
        .setKey(JobKeys.from("role-" + name, "env-" + name, name));
  }
}
