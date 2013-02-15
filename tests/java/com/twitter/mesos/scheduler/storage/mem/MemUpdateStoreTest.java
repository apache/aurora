package com.twitter.mesos.scheduler.storage.mem;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static org.junit.Assert.assertEquals;

public class MemUpdateStoreTest {

  private static final JobUpdateConfiguration CONFIG_A = makeConfig("a");
  private static final JobUpdateConfiguration CONFIG_B = makeConfig("b");
  private static final JobUpdateConfiguration CONFIG_C = makeConfig("c");

  private UpdateStore.Mutable store;

  @Before
  public void setUp() {
    store = new MemUpdateStore();
  }

  @Test
  public void testUpdateStore() {
    assertEquals(
        Optional.<JobUpdateConfiguration>absent(),
        store.fetchJobUpdateConfig("role", "job"));
    assertEquals(ImmutableSet.<JobUpdateConfiguration>of(), store.fetchUpdateConfigs("role"));
    assertEquals(ImmutableSet.<String>of(), store.fetchUpdatingRoles());

    store.saveJobUpdateConfig(CONFIG_A);
    store.saveJobUpdateConfig(CONFIG_B);
    store.saveJobUpdateConfig(CONFIG_C);
    assertEquals(Optional.of(CONFIG_A), store.fetchJobUpdateConfig("role-a", "a"));
    assertEquals(ImmutableSet.of(CONFIG_A), store.fetchUpdateConfigs("role-a"));
    assertEquals(ImmutableSet.of("role-a", "role-b", "role-c"), store.fetchUpdatingRoles());

    store.removeShardUpdateConfigs("role-a", "a");
    store.removeShardUpdateConfigs("role-b", "b");
    assertEquals(
        Optional.<JobUpdateConfiguration>absent(),
        store.fetchJobUpdateConfig("role-a", "a"));
    assertEquals(Optional.of(CONFIG_C), store.fetchJobUpdateConfig("role-c", "c"));
    assertEquals(ImmutableSet.of(CONFIG_C), store.fetchUpdateConfigs("role-c"));
    assertEquals(ImmutableSet.of("role-c"), store.fetchUpdatingRoles());

    store.deleteShardUpdateConfigs();
    assertEquals(
        Optional.<JobUpdateConfiguration>absent(),
        store.fetchJobUpdateConfig("role-c", "c"));
    assertEquals(ImmutableSet.<JobUpdateConfiguration>of(), store.fetchUpdateConfigs("role"));
    assertEquals(ImmutableSet.<String>of(), store.fetchUpdatingRoles());
  }

  private static JobUpdateConfiguration makeConfig(String id) {
    return new JobUpdateConfiguration()
        .setJob(id)
        .setRole("role-" + id)
        .setConfigs(ImmutableSet.of(
            new TaskUpdateConfiguration(
                new TwitterTaskInfo().setJobName(id).setRequestedPorts(ImmutableSet.of("old")),
                new TwitterTaskInfo().setJobName(id).setRequestedPorts(ImmutableSet.of("new")))));
  }
}
