/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.configuration;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.junit.Test;

import static org.apache.aurora.gen.apiConstants.DEFAULT_ENVIRONMENT;
import static org.apache.aurora.gen.test.testConstants.INVALID_IDENTIFIERS;
import static org.apache.aurora.gen.test.testConstants.VALID_IDENTIFIERS;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.isGoodIdentifier;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// TODO(kevints): Improve test coverage for this class.
public class ConfigurationManagerTest {
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
              .setMetadata(null)
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
