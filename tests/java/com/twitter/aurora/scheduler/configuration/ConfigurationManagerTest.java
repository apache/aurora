package com.twitter.aurora.scheduler.configuration;

import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.junit.Test;

import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.CronCollisionPolicy;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.LimitConstraint;
import com.twitter.aurora.gen.TaskConstraint;
import com.twitter.aurora.gen.TwitterTaskInfo;
import com.twitter.aurora.gen.ValueConstraint;
import com.twitter.aurora.scheduler.base.JobKeys;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.aurora.gen.Constants.DEFAULT_ENVIRONMENT;
import static com.twitter.aurora.gen.test.Constants.INVALID_IDENTIFIERS;
import static com.twitter.aurora.gen.test.Constants.VALID_IDENTIFIERS;
import static com.twitter.aurora.scheduler.configuration.ConfigurationManager.isGoodIdentifier;

public class ConfigurationManagerTest {
  private static final String THERMOS_CONFIG = "config";
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
                  .setThermosConfig(THERMOS_CONFIG.getBytes())
                  .setEnvironment(DEFAULT_ENVIRONMENT)
                  .setRequestedPorts(ImmutableSet.<String>of())
                  .setJobName(null)
                  .setPriority(0)
                  .setOwner(null)
                  .setContactEmail("foo@twitter.com")
                  .setProduction(false)
                  .setDiskMb(1)
                  .setPackages(null)
                  .setNumCpus(1.0)
                  .setRamMb(1)
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
          .setRole("owner-role")
          .setUser("owner-user"));


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
  }

  @Test
  public void testApplyDefaultsIfUnsetHeterogeneous() {
    JobConfiguration unclean = new JobConfiguration()
        .setName("jobname")
        .setOwner(new Identity().setRole("role"))
        .setTaskConfigs(ImmutableSet.of(MINIMUM_VIABLE_TASK.deepCopy()));
    ConfigurationManager.applyDefaultsIfUnset(unclean);
    assertEquals(DEFAULT_ENVIRONMENT, unclean.getKey().getEnvironment());
    assertEquals(
        ConfigurationManager.applyDefaultsIfUnset(MINIMUM_VIABLE_TASK), unclean.getTaskConfig());
  }

  @Test
  public void testApplyDefaultsIfUnsetUnsanitized() {
    JobConfiguration copy = UNSANITIZED_JOB_CONFIGURATION.deepCopy();
    ConfigurationManager.applyDefaultsIfUnset(copy);
    assertTrue(copy.isSetKey());
    assertEquals(DEFAULT_ENVIRONMENT, copy.getKey().getEnvironment());
  }

  @Test
  public void testTaskConfigBackfillEqualTasks() {
    // TODO(Sathya): Remove this after deploying MESOS-3048.
    TwitterTaskInfo commonTask = MINIMUM_VIABLE_TASK.deepCopy()
        .setThermosConfig(THERMOS_CONFIG.getBytes());
    Set<TwitterTaskInfo> tasks =
        ImmutableSet.of(commonTask.setShardId(0), commonTask.deepCopy().setShardId(1));
    JobConfiguration copy = UNSANITIZED_JOB_CONFIGURATION.deepCopy()
        .setTaskConfigs(tasks);
    ConfigurationManager.applyDefaultsIfUnset(copy);
    assertTrue(copy.isSetTaskConfig());
    assertEquals(ConfigurationManager.applyDefaultsIfUnset(commonTask), copy.getTaskConfig());
    assertEquals(2, copy.getShardCount());
  }

  @Test
  public void testTaskConfigBackfillUniqueTasks() {
    // TODO(Sathya): Remove this after deploying MESOS-3048.
    TwitterTaskInfo commonTask = MINIMUM_VIABLE_TASK.deepCopy()
        .setThermosConfig(THERMOS_CONFIG.getBytes())
        .setShardId(0);
    TwitterTaskInfo uniqueTask = MINIMUM_VIABLE_TASK.deepCopy()
        .setThermosConfig("new config".getBytes())
        .setShardId(1);
    JobConfiguration copy = UNSANITIZED_JOB_CONFIGURATION.deepCopy()
        .setTaskConfigs(ImmutableSet.of(commonTask, uniqueTask));
    ConfigurationManager.applyDefaultsIfUnset(copy);
    assertFalse(copy.isSetTaskConfig());
    assertEquals(0, copy.getShardCount());
  }

  @Test
  public void testFillsJobKeyFromConfig() throws Exception {
    JobConfiguration copy = UNSANITIZED_JOB_CONFIGURATION.deepCopy();
    copy.unsetKey();
    copy.setTaskConfig(Iterables.getFirst(copy.getTaskConfigs(), null));
    copy.getTaskConfig().unsetShardId();
    copy.setShardCount(1);
    copy.unsetTaskConfigs();
    ConfigurationManager.validateAndPopulate(copy);
    assertEquals(
        JobKeys.from(copy.getOwner().getRole(), DEFAULT_ENVIRONMENT, copy.getName()),
        copy.getKey());
  }
}
