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
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.gen.AppcImage;
import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.CoordinatorSlaPolicy;
import org.apache.aurora.gen.CountSlaPolicy;
import org.apache.aurora.gen.CronCollisionPolicy;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.Image;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.LimitConstraint;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.MesosFetcherURI;
import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.PercentageSlaPolicy;
import org.apache.aurora.gen.SlaPolicy;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.gen.ValueConstraint;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.gen.apiConstants;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.ConfigurationManagerSettings;
import org.apache.aurora.scheduler.configuration.ConfigurationManager.TaskDescriptionException;
import org.apache.aurora.scheduler.mesos.TestExecutorSettings;
import org.apache.aurora.scheduler.storage.entities.IDockerParameter;
import org.apache.aurora.scheduler.storage.entities.IJobConfiguration;
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
import static org.apache.aurora.scheduler.configuration.ConfigurationManager.NO_CONTAINER_VOLUMES;
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
  private static final int MIN_REQUIRED_INSTANCES = 20;
  private static final long MAX_SLA_DURATION_SECS = 7200;
  private static final long DEFAULT_SLA_PERCENTAGE = 95;

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
              .setExecutorConfig(new ExecutorConfig(apiConstants.AURORA_EXECUTOR_NAME, "config"))
              .setPriority(0)
              .setOwner(null)
              .setContactEmail("foo@twitter.com")
              .setProduction(false)
              .setMetadata(null)
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
          ImmutableList.of(),
          true,
          false,
          true,
          false,
          MIN_REQUIRED_INSTANCES,
          MAX_SLA_DURATION_SECS,
          ConfigurationManager.DEFAULT_ALLOWED_JOB_ENVIRONMENTS,
          false),
      TaskTestUtil.TIER_MANAGER,
      TaskTestUtil.THRIFT_BACKFILL,
      TestExecutorSettings.THERMOS_EXECUTOR);
  private static final ConfigurationManager DOCKER_CONFIGURATION_MANAGER = new ConfigurationManager(
      new ConfigurationManagerSettings(
          ALL_CONTAINER_TYPES,
          true,
          ImmutableList.of(new DockerParameter("foo", "bar")),
          false,
          true,
          true,
          true,
          MIN_REQUIRED_INSTANCES,
          MAX_SLA_DURATION_SECS,
          ConfigurationManager.DEFAULT_ALLOWED_JOB_ENVIRONMENTS,
          false),
      TaskTestUtil.TIER_MANAGER,
      TaskTestUtil.THRIFT_BACKFILL,
      TestExecutorSettings.THERMOS_EXECUTOR);

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
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(taskConfig), 100);
  }

  @Test
  public void testDisallowedDockerParameters() throws TaskDescriptionException {
    TaskConfig taskConfig = CONFIG_WITH_CONTAINER.newBuilder();
    taskConfig.getContainer().getDocker().addToParameters(new DockerParameter("foo", "bar"));

    expectTaskDescriptionException(ConfigurationManager.NO_DOCKER_PARAMETERS);
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(taskConfig), 100);
  }

  @Test
  public void testDisallowNoExecutorDockerTask() throws TaskDescriptionException {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.getContainer().getDocker().unsetParameters();
    builder.unsetExecutorConfig();

    expectTaskDescriptionException(ConfigurationManager.EXECUTOR_REQUIRED_WITH_DOCKER);
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 1);
  }

  @Test
  public void testAllowNoExecutorDockerTask() throws TaskDescriptionException {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.unsetExecutorConfig();

    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testInvalidTier() throws TaskDescriptionException {
    ITaskConfig config = ITaskConfig.build(UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig()
        .setTier("pr/d"));

    expectTaskDescriptionException("Tier contains illegal characters");
    CONFIGURATION_MANAGER.validateAndPopulate(config, 100);
  }

  @Test
  public void testDefaultDockerParameters() throws TaskDescriptionException {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.getContainer().getDocker().setParameters(ImmutableList.of());

    ITaskConfig result =
        DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);

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
        ITaskConfig.build(taskConfig), 100);

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

    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testNonExclusiveDedicatedAllowed() throws Exception {
    TaskConfig builder = UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig();
    builder.setConstraints(ImmutableSet.of(new Constraint()
        .setName(DEDICATED_ATTRIBUTE)
        .setConstraint(TaskConstraint.value(new ValueConstraint(false, ImmutableSet.of("*/f"))))));

    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testExclusiveDedicatedRoleMismatch() throws Exception {
    TaskConfig builder = UNSANITIZED_JOB_CONFIGURATION.deepCopy().getTaskConfig();
    builder.setConstraints(ImmutableSet.of(new Constraint()
        .setName(DEDICATED_ATTRIBUTE)
        .setConstraint(TaskConstraint.value(new ValueConstraint(false, ImmutableSet.of("r/f"))))));

    expectTaskDescriptionException("Only r may use hosts dedicated for that role.");
    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testMultipleResourceValuesBlocked() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.addToResources(numCpus(3.0));
    builder.addToResources(ramMb(72));

    expectTaskDescriptionException("Multiple resource values are not supported for CPU, RAM");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testMultipleResourceValuesAllowed() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.addToResources(namedPort("thrift"));

    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
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
            ImmutableList.of(new DockerParameter("foo", "bar")),
            false,
            false,
            false,
            false,
            MIN_REQUIRED_INSTANCES,
            MAX_SLA_DURATION_SECS,
            ConfigurationManager.DEFAULT_ALLOWED_JOB_ENVIRONMENTS,
            false),
        TaskTestUtil.TIER_MANAGER,
        TaskTestUtil.THRIFT_BACKFILL,
        TestExecutorSettings.THERMOS_EXECUTOR).validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testMesosFetcherDisabled() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.setMesosFetcherUris(
        ImmutableSet.of(
            new MesosFetcherURI("pathA").setExtract(true).setCache(true),
            new MesosFetcherURI("pathB").setExtract(true).setCache(true)));

    expectTaskDescriptionException(ConfigurationManager.MESOS_FETCHER_DISABLED);
    new ConfigurationManager(
            new ConfigurationManagerSettings(
                    ALL_CONTAINER_TYPES,
                    true,
                    ImmutableList.of(new DockerParameter("foo", "bar")),
                    false,
                    false,
                    false,
                    false,
                    MIN_REQUIRED_INSTANCES,
                    MAX_SLA_DURATION_SECS,
                    ".+",
                    false),
            TaskTestUtil.TIER_MANAGER,
            TaskTestUtil.THRIFT_BACKFILL,
            TestExecutorSettings.THERMOS_EXECUTOR)
        .validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testContainerVolumesDisabled() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    MesosContainer container = new MesosContainer()
        .setImage(Image.appc(new AppcImage("name", "id")))
        .setVolumes(ImmutableList.of(new Volume("container", "host", Mode.RO)));
    builder.setContainer(Container.mesos(container));

    expectTaskDescriptionException(NO_CONTAINER_VOLUMES);

    CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
  }

  @Test
  public void testTaskLinks() throws Exception {
    TaskConfig builder = CONFIG_WITH_CONTAINER.newBuilder();
    builder.addToResources(namedPort("health"));
    builder.unsetTaskLinks();

    ITaskConfig populated =
        DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(ITaskConfig.build(builder), 100);
    assertEquals(ImmutableSet.of("health", "http"), populated.getTaskLinks().keySet());
  }

  @Test
  public void testJobEnvironmentValidation() throws Exception {
    JobConfiguration jobConfiguration = UNSANITIZED_JOB_CONFIGURATION.deepCopy();
    jobConfiguration.getKey().setEnvironment("foo");
    expectTaskDescriptionException("Job environment foo doesn't match: b.r");
    new ConfigurationManager(
      new ConfigurationManagerSettings(
          ALL_CONTAINER_TYPES,
          true,
          ImmutableList.of(new DockerParameter("foo", "bar")),
          false,
          true,
          true,
          true,
          MIN_REQUIRED_INSTANCES,
          MAX_SLA_DURATION_SECS,
          "b.r",
          false),
      TaskTestUtil.TIER_MANAGER,
      TaskTestUtil.THRIFT_BACKFILL,
      TestExecutorSettings.THERMOS_EXECUTOR)
            .validateAndPopulate(IJobConfiguration.build(jobConfiguration));
  }

  @Test
  public void testSlaPolicyNonProdTiersEnabled() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setTier(TaskTestUtil.DEV_TIER_NAME)
            .setSlaPolicy(SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setCount(MIN_REQUIRED_INSTANCES - 1))));

    new ConfigurationManager(
        new ConfigurationManagerSettings(
            ALL_CONTAINER_TYPES,
            true,
            ImmutableList.of(new DockerParameter("foo", "bar")),
            false,
            true,
            true,
            true,
            MIN_REQUIRED_INSTANCES,
            MAX_SLA_DURATION_SECS,
            ConfigurationManager.DEFAULT_ALLOWED_JOB_ENVIRONMENTS,
            true),
        TaskTestUtil.TIER_MANAGER,
        TaskTestUtil.THRIFT_BACKFILL,
        TestExecutorSettings.THERMOS_EXECUTOR).validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testCountSlaPolicyUnsupportedTier() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setTier(TaskTestUtil.DEV_TIER_NAME)
            .setSlaPolicy(SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setCount(MIN_REQUIRED_INSTANCES))));

    expectTaskDescriptionException("Tier 'tier-dev' does not support SlaPolicy.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testCountSlaPolicyTooFewInstances() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setSlaPolicy(SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setCount(MIN_REQUIRED_INSTANCES))));

    expectTaskDescriptionException(
        "Job with fewer than 20 instances cannot have Percentage/Count SlaPolicy.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES - 1);
  }

  @Test
  public void testCountSlaPolicyCountTooHigh() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setSlaPolicy(SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                  .setCount(MIN_REQUIRED_INSTANCES))));

    expectTaskDescriptionException(
        "Current CountSlaPolicy: count=20 will not allow any instances to be killed. "
            + "Must be less than instanceCount=20.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testCountSlaPolicyDurationSecsTooHigh() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setSlaPolicy(SlaPolicy.countSlaPolicy(
                new CountSlaPolicy()
                    .setDurationSecs(MAX_SLA_DURATION_SECS + 1))));

    expectTaskDescriptionException("CountSlaPolicy: durationSecs=7201 must be less than "
        + "cluster-wide maximum of 7200 secs.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testPercentageSlaPolicyUnsupportedTier() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setTier(TaskTestUtil.DEV_TIER_NAME)
            .setSlaPolicy(SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setPercentage(DEFAULT_SLA_PERCENTAGE))));

    expectTaskDescriptionException("Tier 'tier-dev' does not support SlaPolicy.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testPercentageSlaPolicyTooFewInstances() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setSlaPolicy(SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setPercentage(DEFAULT_SLA_PERCENTAGE))));

    expectTaskDescriptionException(
        "Job with fewer than 20 instances cannot have Percentage/Count SlaPolicy.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES - 1);
  }

  @Test
  public void testPercentageSlaPolicyPercentageTooHigh() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setSlaPolicy(SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setPercentage(100))));

    expectTaskDescriptionException(
        "Current PercentageSlaPolicy: percentage=100.000000 will not allow any instances "
            + "to be killed. Must be less than 95.000000.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testPercentageSlaPolicyDurationSecsTooHigh() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setSlaPolicy(SlaPolicy.percentageSlaPolicy(
                new PercentageSlaPolicy()
                    .setDurationSecs(MAX_SLA_DURATION_SECS + 1))));

    expectTaskDescriptionException("PercentageSlaPolicy: durationSecs=7201 must be less than "
        + "cluster-wide maximum of 7200 secs.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testCoordinatorSlaPolicyUnsupportedTier() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setTier(TaskTestUtil.DEV_TIER_NAME)
            .setSlaPolicy(SlaPolicy.coordinatorSlaPolicy(
                new CoordinatorSlaPolicy()
                    .setCoordinatorUrl("http://localhost"))));

    expectTaskDescriptionException("Tier 'tier-dev' does not support SlaPolicy.");
    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES);
  }

  @Test
  public void testCoordinatorSlaPolicyTooFewInstances() throws Exception {
    ITaskConfig config = ITaskConfig.build(
        CONFIG_WITH_CONTAINER
            .newBuilder()
            .setSlaPolicy(SlaPolicy.coordinatorSlaPolicy(
                new CoordinatorSlaPolicy()
                    .setCoordinatorUrl("http://localhost"))));

    DOCKER_CONFIGURATION_MANAGER.validateAndPopulate(
        config,
        MIN_REQUIRED_INSTANCES - 1);
  }

  private void expectTaskDescriptionException(String message) {
    expectedException.expect(TaskDescriptionException.class);
    expectedException.expectMessage(message);
  }
}
