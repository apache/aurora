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
package com.twitter.aurora.scheduler.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import com.twitter.aurora.gen.Constraint;
import com.twitter.aurora.gen.CronCollisionPolicy;
import com.twitter.aurora.gen.ExecutorConfig;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.LimitConstraint;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.gen.TaskConstraint;
import com.twitter.aurora.gen.ValueConstraint;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static com.twitter.aurora.gen.apiConstants.DEFAULT_ENVIRONMENT;
import static com.twitter.aurora.gen.test.testConstants.INVALID_IDENTIFIERS;
import static com.twitter.aurora.gen.test.testConstants.VALID_IDENTIFIERS;
import static com.twitter.aurora.scheduler.configuration.ConfigurationManager.isGoodIdentifier;

// TODO(Sathya): Improve test coverage for this class.
public class ConfigurationManagerTest {
  // This job caused a crash when loaded in MESOS-3062
  // TODO(ksweeney): Create a test fixtures resource file and move this to it.
  private static final JobConfiguration UNSANITIZED_JOB_CONFIGURATION = new JobConfiguration()
      .setKey(new JobKey("owner-role", DEFAULT_ENVIRONMENT, "email_stats"))
      .setCronSchedule("0 2 * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setInstanceCount(1)
      .setTaskConfig(
          new TaskConfig()
              .setIsService(false)
              .setTaskLinks(ImmutableMap.<String, String>of())
              .setExecutorConfig(new ExecutorConfig("aurora", "config"))
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
                              .setLimit(1))))))
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
  public void testApplyDefaultsIfUnsetUnsanitized() {
    JobConfiguration copy = UNSANITIZED_JOB_CONFIGURATION.deepCopy();

    ConfigurationManager.applyDefaultsIfUnset(copy);
    assertTrue(copy.isSetKey());
    assertEquals(DEFAULT_ENVIRONMENT, copy.getKey().getEnvironment());
  }
}
