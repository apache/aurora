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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.ConfigurationManagerSettings;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.storage.entities.IDockerParameter;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.namedPort;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.numGpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.apache.aurora.gen.test.testConstants.INVALID_IDENTIFIERS;
import static org.apache.aurora.gen.test.testConstants.VALID_IDENTIFIERS;
import static org.apache.aurora.scheduler.base.UserProvidedStrings.isGoodIdentifier;
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.DEDICATED_ATTRIBUTE;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

// TODO(kevints): Improve test coverage for this class.
public class ConfigurationManagerTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final ImmutableSet<Container._Fields> ALL_CONTAINER_TYPES =
      ImmutableSet.copyOf(Container._Fields.values());

  private static final JobKey JOB_KEY = new JobKey("owner-role", "devel", "email_stats");
  private static final JobConfiguration UNSANITIZED_JOB_CONFIGURATION = new JobConfiguration()
      .setKey(JOB_KEY)
      .setCronSchedule("0 2 * * *")
      .setCronCollisionPolicy(CronCollisionPolicy.KILL_EXISTING)
      .setInstanceCount(1)
      .setTaskConfig(
          new TaskConfig()
              .setJob(JOB_KEY)
              .setIsService(false)
              .setTaskLinks(ImmutableMap.of())
              .setExecutorConfig(new ExecutorConfig("aurora", "config"))
              .setRequestedPorts(ImmutableSet.of())
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
              .setOwner(new Identity().setUser("owner-user"))
              .setResources(ImmutableSet.of(
                  numCpus(1.0),
                  ramMb(1),
                  diskMb(1))));
  private static final ITaskConfig CONFIG_WITH_CONTAINER =
      TaskTestUtil.makeConfig(JobKeys.from("role", "env", "job"));

  private static final ConfigurationManager CONFIGURATION_MANAGER = new ConfigurationManager(
      new ConfigurationManagerSettings(
          ALL_CONTAINER_TYPES,
          false,
          ImmutableMultimap.of(),
          true,
          false),
      TaskTestUtil.DEV_TIER_MANAGER);
  private static final ConfigurationManager DOCKER_CONFIGURATION_MANAGER = new ConfigurationManager(
      new ConfigurationManagerSettings(
          ALL_CONTAINER_TYPES,
          true,
          ImmutableMultimap.of("foo", "bar"),
          false,
          true),
      TaskTestUtil.DEV_TIER_MANAGER);

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
  public void testBadContainerConfig() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.newBuilder();
    taskConfig.getContainer().getDocker().setImage(null);

    expectTaskDescriptionException("A container must specify an image");
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(taskConfig));
  }

  @Test
  public void testDisallowedDockerParameters() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.newBuilder();
    taskConfig.getContainer().getDocker().addToParameters(new DockerParameter("foo", "bar"));

    expectTaskDescriptionException(ConfigurationManager.NO_DOCKER_PARAMETERS);
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(taskConfig));
  }

  @Test
  public void testDisallowNoExecutorDockerTask() throws TaskDescriptionException {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.getContainer().getDocker().unsetParameters();
    builder.unsetExecutorConfig();

    expectTaskDescriptionException(ConfigurationManager.EXECUTOR_REQUIRED_WITH_DOCKER);
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testAllowNoExecutorDockerTask() throws TaskDescriptionException {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.unsetExecutorConfig();

    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testInvalidTier() throws TaskDescriptionException {
    ITaskConfig config = ITaskConfig.build(UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig()
        .setTier("pr/d"));

    expectTaskDescriptionException("Tier contains illegal characters");
    CONFIGURATION_MANAGER.validateAndPopulate(config);
  }

  @Test
  public void testDefaultDockerParameters() throws TaskDescriptionException {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.getContainer().getDocker().setParameters(ImmutableList.of());

    ITaskConfig result =
        DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));

    // The resulting task config should contain parameters supplied to the ConfigurationManager.
    List<IDockerParameter> params = result.getContainer().getDocker().getParameters();
    assertThat(
        params, is(ImmutableList.of(IDockerParameter.build(new DockerParameter("foo", "bar")))));
  }

  @Test
  public void testPassthroughDockerParameters() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.newBuilder();
    DockerParameter userParameter = new DockerParameter("bar", "baz");
    taskConfig.getContainer().getDocker().getParameters().clear();
    taskConfig.getContainer().getDocker().addToParameters(userParameter);

    ITaskConfig result = DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        ITaskConfig.build(taskConfig));

    // The resulting task config should contain parameters supplied from user config.
    List<IDockerParameter> params = result.getContainer().getDocker().getParameters();
    assertThat(params, is(ImmutableList.of(IDockerParameter.build(userParameter))));
  }

  @Test
  public void testExclusiveDedicatedRoleAllowed() throws Exception {
    TaskConfig builder = UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig();
    builder.setConstraints(ImmutableSet.of(new Constraint()
        .setName(DEDICATED_ATTRIBUTE)
        .setConstraint(TaskConstraint.value(
            new ValueConstraint(false, ImmutableSet.of(JOB_KEY.getRole() + "/f"))))));

    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testNonExclusiveDedicatedAllowed() throws Exception {
    TaskConfig builder = UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig();
    builder.setConstraints(ImmutableSet.of(new Constraint()
        .setName(DEDICATED_ATTRIBUTE)
        .setConstraint(TaskConstraint.value(new ValueConstraint(false, ImmutableSet.of("*/f"))))));

    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testExclusiveDedicatedRoleMismatch() throws Exception {
    TaskConfig builder = UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig();
    builder.setConstraints(ImmutableSet.of(new Constraint()
        .setName(DEDICATED_ATTRIBUTE)
        .setConstraint(TaskConstraint.value(new ValueConstraint(false, ImmutableSet.of("r/f"))))));

    expectTaskDescriptionException("Only r may use hosts dedicated for that role.");
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testTaskResourceBackfill() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.unsetResources();

    assertFalse(builder.isSetResources());
    ITaskConfig populated =
        DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
    assertEquals(CONFIG_WITH_CONTAINER.getResources(), populated.getResources());
  }

  @Test
  public void testMultipleResourceValuesBlocked() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.addToResources(numCpus(3.0));
    builder.addToResources(ramMb(72));

    expectTaskDescriptionException("Multiple resource values are not supported for CPU, RAM");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testMultipleResourceValuesAllowed() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.addToResources(namedPort("thrift"));

    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testGpuResourcesNotAllowed() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.addToResources(numGpus(2));

    expectTaskDescriptionException("GPU resource support is disabled in this cluster.");
    new ConfigurationManager(
        new ConfigurationManagerSettings(
            ALL_CONTAINER_TYPES,
            true,
            ImmutableMultimap.of("foo", "bar"),
            false,
            false),
        TaskTestUtil.DEV_TIER_MANAGER).validateAndPopulate(ITaskConfig.build(builder));
  }

  @Test
  public void testTaskLinks() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.addToResources(namedPort("health"));
    builder.unsetTaskLinks();

    ITaskConfig populated =
        DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder));
    assertEquals(ImmutableSet.of("health", "http"), populated.getTaskLinks().keySet());
  }

  private void expectTaskDescriptionException(String message) {
    expectedException.expect(TaskDescriptionException.class);
    expectedException.expectMessage(message);
  }
}
