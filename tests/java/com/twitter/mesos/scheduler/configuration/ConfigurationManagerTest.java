package com.twitter.mesos.scheduler.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Test;

import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.CronCollisionPolicy;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.LimitConstraint;
import com.twitter.mesos.gen.TaskConstraint;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.gen.ValueConstraint;

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

  // This job caused a crash when loaded in MESOS-3062
  // TODO(ksweeney): Create a test fixtures resource file and move this to it.
  private static final JobConfiguration UNSANITIZED_JOB_CONFIGURATION = new JobConfiguration()
      .setName("email_stats")
      .setCronSchedule("0 2 * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setShardCount(0)
      .setTaskConfigs(
          ImmutableSet.of(
              new TwitterTaskInfo()
                  .setIsService(false)
                  .setHealthCheckIntervalSecs(0)
                  .setTaskLinks(ImmutableMap.<String, String>of())
                  .setThermosConfig(new byte[] {})
                  .setEnvironment(null)
                  .setRequestedPorts(ImmutableSet.<String>of())
                  .setJobName(null)
                  .setPriority(0)
                  .setOwner(null)
                  .setContactEmail(null)
                  .setProduction(false)
                  .setDiskMb(0L)
                  .setPackages(null)
                  .setRamMb(0L)
                  .setMaxTaskFailures(0)
                  .setShardId(0)
                  .setConstraints(
                      ImmutableSet.of(
                          new Constraint()
                              .setName("executor")
                              .setConstraint(TaskConstraint
                                  .value(new ValueConstraint()
                                      .setNegated(false)
                                      .setValues(ImmutableSet.of("legacy")))),
                          new Constraint()
                              .setName("host")
                              .setConstraint(TaskConstraint.limit(new LimitConstraint()
                                      .setLimit(1)))))))
      .setKey(null)
      .setTaskConfig(null)
      .setOwner(new Identity()
          .setRole("nikhil")
          .setUser("nikhil"));


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

  @Test
  public void testApplyDefaultsIfUnsetUnsanitized() {
    JobConfiguration copy = UNSANITIZED_JOB_CONFIGURATION.deepCopy();
    ConfigurationManager.applyDefaultsIfUnset(copy);
    assertTrue(copy.isSetKey());
    assertEquals(DEFAULT_ENVIRONMENT, copy.getKey().getEnvironment());
  }
}
