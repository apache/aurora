/**
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
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.test.testConstants.INVALID_IDENTIFIERS;
import static org.apache.aurora.gen.test.testConstants.VALID_IDENTIFIERS;
import static org.apache.aurora.scheduler.base.UserProvidedStrings.isGoodIdentifier;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// TODO(kevints): Improve test coverage for this class.
public class ConfigurationManagerTest {
  private static final JobConfiguration UNSANITIZED_JOB_CONFIGURATION = new JobConfiguration()
      .setKey(new JobKey("owner-role", "devel", "email_stats"))
      .setCronSchedule("0 2 * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setInstanceCount(1)
      .setTaskConfig(
          new TaskConfig()
              .setIsService(false)
              .setTaskLinks(ImmutableMap.of())
              .setExecutorConfig(new ExecutorConfig("aurora", "config"))
              .setEnvironment("devel")
              .setRequestedPorts(ImmutableSet.of())
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
                              .setLimit(1))),
                      new Constraint()
                          .setName(DEDICATED_ATTRIBUTE)
                          .setConstraint(TaskConstraint.value(new ValueConstraint(
                              false, ImmutableSet.of("foo"))))))
              .setOwner(new Identity()
                  .setRole("owner-role")
                  .setUser("owner-user")));
  private static final TaskConfig CONFIG_WITH_CONTAINER = ITaskConfig.build(new TaskConfig()
      .setJobName("container-test")
      .setEnvironment("devel")
      .setExecutorConfig(new ExecutorConfig())
      .setOwner(new Identity("role", "user"))
      .setNumCpus(1)
      .setRamMb(1)
      .setDiskMb(1)
      .setContainer(Container.docker(new DockerContainer("testimage"))))
      .newBuilder();

  private ConfigurationManager configurationManager;

  @Before
  public void setUp() {
    configurationManager = new ConfigurationManager(ImmutableSet.of(), false);
  }

  @Test
  public void testIsGoodIdentifier() {
    for (String identifier : VALID_IDENTIFIERS) {
      assertTrue(isGoodIdentifier(identifier));
    }
    for (String identifier : INVALID_IDENTIFIERS) {
      assertFalse(isGoodIdentifier(identifier));
    }
  }

  @Test(expected = TaskDescriptionException.class)
  public void testBadContainerConfig() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.deepCopy();
    taskConfig.getContainer().getDocker().setImage(null);

    configurationManager.validateAndPopulate(ITaskConfig.build(taskConfig));
  }

  @Test(expected = TaskDescriptionException.class)
  public void testInvalidTier() throws TaskDescriptionException {
    ITaskConfig config = ITaskConfig.build(UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig()
        .setJobName("job")
        .setEnvironment("env")
        .setTier("pr/d"));

    configurationManager.validateAndPopulate(config);
  }
}
