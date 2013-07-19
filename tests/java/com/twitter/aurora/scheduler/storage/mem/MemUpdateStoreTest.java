package com.twitter.aurora.scheduler.storage.mem;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.JobUpdateConfiguration;
import com.twitter.aurora.gen.TaskUpdateConfiguration;
import com.twitter.aurora.gen.TwitterTaskInfo;
import com.twitter.aurora.scheduler.base.JobKeys;
import com.twitter.aurora.scheduler.storage.UpdateStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class MemUpdateStoreTest {
  private static final JobUpdateConfiguration CONFIG_A = makeConfig("a");
  private static final JobUpdateConfiguration CONFIG_B = makeConfig("b");
  private static final JobUpdateConfiguration CONFIG_C = makeConfig("c");

  private static final JobKey KEY_A = CONFIG_A.getJobKey();
  private static final JobKey KEY_B = CONFIG_B.getJobKey();
  private static final JobKey KEY_C = CONFIG_C.getJobKey();

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

    assertNull(store.fetchJobUpdateConfig(staging.getJobKey().deepCopy().setEnvironment("devel"))
        .orNull());
    assertEquals(staging, store.fetchJobUpdateConfig(staging.getJobKey()).orNull());
    assertEquals(prod, store.fetchJobUpdateConfig(prod.getJobKey()).orNull());

    store.removeShardUpdateConfigs(staging.getJobKey());
    assertNull(store.fetchJobUpdateConfig(staging.getJobKey()).orNull());
    assertEquals(prod, store.fetchJobUpdateConfig(prod.getJobKey()).orNull());
  }

  private static JobKey makeKey(String id) {
    return JobKeys.from("role-" + id, "env-" + id, id);
  }

  private static JobUpdateConfiguration makeConfig(String id) {
    return makeConfig(makeKey(id));
  }

  private static JobUpdateConfiguration makeConfig(JobKey jobKey) {
    TwitterTaskInfo template = new TwitterTaskInfo()
        .setOwner(new Identity().setRole(jobKey.getRole()).setUser("user-" + jobKey.getName()))
        .setEnvironment(jobKey.getEnvironment())
        .setJobName(jobKey.getName());

    return new JobUpdateConfiguration().setJobKey(jobKey)
        .setConfigs(ImmutableSet.of(
            new TaskUpdateConfiguration(
                template.deepCopy().setRequestedPorts(ImmutableSet.of("old")),
                template.deepCopy().setRequestedPorts(ImmutableSet.of("new")))));
  }
}
