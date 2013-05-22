package com.twitter.mesos.scheduler.configuration;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Test;

import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.TwitterTaskInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.mesos.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.mesos.gen.test.Constants.INVALID_IDENTIFIERS;
import static com.twitter.mesos.gen.test.Constants.VALID_IDENTIFIERS;

import static com.twitter.mesos.scheduler.configuration.ConfigurationManager.isGoodIdentifier;

public class ConfigurationManagerTest {
  private static final TwitterTaskInfo MINIMUM_VIABLE_TASK = new TwitterTaskInfo()
      .setNumCpus(1.0)
      .setRamMb(64)
      .setDiskMb(64);

  @Test
  public void testIsGoodIdentifier() {
    for (String identifier : VALID_IDENTIFIERS) {
      assertTrue(isGoodIdentifier(identifier));
    }
    for (String identifier : INVALID_IDENTIFIERS) {
      assertFalse(isGoodIdentifier(identifier));
    }
  }

  @Test
  public void testApplyDefaultsIfUnsetHomogeneous() {
    JobConfiguration unclean = new JobConfiguration()
        .setName("jobname")
        .setOwner(new Identity().setRole("role"))
        .setTaskConfig(MINIMUM_VIABLE_TASK.deepCopy())
        .setTaskConfigs(ImmutableSet.of(MINIMUM_VIABLE_TASK.deepCopy()));
    ConfigurationManager.applyDefaultsIfUnset(unclean);
    assertEquals(DEFAULT_ENVIRONMENT, unclean.getKey().getEnvironment());
    assertEquals(DEFAULT_ENVIRONMENT, unclean.getTaskConfig().getEnvironment());
    assertEquals(
        DEFAULT_ENVIRONMENT, Iterables.getOnlyElement(unclean.getTaskConfigs()).getEnvironment());
  }

  @Test
  public void testApplyDefaultsIfUnsetHeterogeneous() {
    JobConfiguration unclean = new JobConfiguration()
        .setName("jobname")
        .setOwner(new Identity().setRole("role"))
        .setTaskConfigs(ImmutableSet.of(MINIMUM_VIABLE_TASK.deepCopy()));
    ConfigurationManager.applyDefaultsIfUnset(unclean);
    assertEquals(DEFAULT_ENVIRONMENT,
        Iterables.getOnlyElement(unclean.getTaskConfigs()).getEnvironment());
    assertEquals(DEFAULT_ENVIRONMENT, unclean.getKey().getEnvironment());
  }
}
