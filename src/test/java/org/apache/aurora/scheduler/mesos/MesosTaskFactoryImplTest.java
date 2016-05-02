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
package org.apache.aurora.scheduler.mesos;

import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.executor.ExecutorConfig;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.resources.ResourceSlot;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.resources.Resources;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerInfo;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.ContainerInfo.Type;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Parameter;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Volume;
import org.apache.mesos.Protos.Volume.Mode;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.base.TaskTestUtil.DEV_TIER;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.DEFAULT_PORT_PROTOCOL;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.METADATA_LABEL_PREFIX;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.getInverseJobSourceName;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.SOME_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TestExecutorSettings.THERMOS_CONFIG;
import static org.apache.aurora.scheduler.mesos.TestExecutorSettings.THERMOS_EXECUTOR;
import static org.apache.aurora.scheduler.resources.ResourceSlot.makeMesosRangeResource;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.resetPorts;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MesosTaskFactoryImplTest extends EasyMockTest {

  private static final ITaskConfig TASK_CONFIG = ITaskConfig.build(
      TaskTestUtil.makeConfig(TaskTestUtil.JOB)
          .newBuilder()
          .setContainer(Container.mesos(new MesosContainer())));
  private static final IAssignedTask TASK = IAssignedTask.build(new AssignedTask()
      .setInstanceId(2)
      .setTaskId("task-id")
      .setAssignedPorts(ImmutableMap.of("http", 80))
      .setTask(TASK_CONFIG.newBuilder()));
  private static final IAssignedTask TASK_WITH_DOCKER = IAssignedTask.build(TASK.newBuilder()
      .setTask(
          new TaskConfig(TASK.getTask().newBuilder())
              .setContainer(Container.docker(
                  new DockerContainer("testimage")))));
  private static final IAssignedTask TASK_WITH_DOCKER_PARAMS = IAssignedTask.build(TASK.newBuilder()
      .setTask(
          new TaskConfig(TASK.getTask().newBuilder())
              .setContainer(Container.docker(
                  new DockerContainer("testimage").setParameters(
                      ImmutableList.of(new DockerParameter("label", "testparameter")))))));

  private static final SlaveID SLAVE = SlaveID.newBuilder().setValue("slave-id").build();
  private static final Offer OFFER_THERMOS_EXECUTOR = Protos.Offer.newBuilder()
      .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
      .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
      .setSlaveId(SLAVE)
      .setHostname("slave-hostname")
      .addAllResources(
          ResourceSlot.from(TASK_CONFIG).add(THERMOS_EXECUTOR.getExecutorOverhead())
              .toResourceList(DEV_TIER))
      .addResources(makeMesosRangeResource(ResourceType.PORTS, ImmutableSet.of(80)))
      .build();
  private static final Offer OFFER_SOME_OVERHEAD_EXECUTOR = OFFER_THERMOS_EXECUTOR.toBuilder()
      .clearResources()
      .addAllResources(
              ResourceSlot.from(TASK_CONFIG).add(SOME_OVERHEAD_EXECUTOR.getExecutorOverhead())
                      .toResourceList(DEV_TIER))
      .addResources(makeMesosRangeResource(ResourceType.PORTS, ImmutableSet.of(80)))
      .build();

  private static final String CLUSTER_NAME = "cluster_name";
  private static final IServerInfo SERVER_INFO = IServerInfo.build(
      new ServerInfo(CLUSTER_NAME, ""));

  private MesosTaskFactory taskFactory;
  private ExecutorSettings config;
  private TierManager tierManager;

  private static final ExecutorInfo DEFAULT_EXECUTOR = THERMOS_CONFIG.getExecutor();

  @Before
  public void setUp() {
    config = THERMOS_EXECUTOR;
    tierManager = createMock(TierManager.class);
  }

  private static ExecutorInfo populateDynamicFields(ExecutorInfo executor, IAssignedTask task) {
    return executor.toBuilder()
        .setExecutorId(MesosTaskFactoryImpl.getExecutorId(task.getTaskId()))
        .setSource(
            MesosTaskFactoryImpl.getInstanceSourceName(task.getTask(), task.getInstanceId()))
        .build();
  }

  private static ExecutorInfo purgeZeroResources(ExecutorInfo executor) {
    return executor.toBuilder()
        .clearResources()
        .addAllResources(
            executor.getResourcesList()
                .stream()
                .filter(
                    e -> !e.hasScalar() || e.getScalar().getValue() > 0)
                .collect(Collectors.toList()))
        .build();
  }

  @Test
  public void testExecutorInfoUnchanged() {
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR);

    assertEquals(populateDynamicFields(DEFAULT_EXECUTOR, TASK), task.getExecutor());
    checkTaskResources(TASK.getTask(), task);
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testTaskInfoRevocable() {
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(REVOCABLE_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    Resource revocableCPU = OFFER_THERMOS_EXECUTOR.getResources(0).toBuilder()
        .setRevocable(Resource.RevocableInfo.getDefaultInstance())
        .build();
    Offer withRevocable = OFFER_THERMOS_EXECUTOR.toBuilder()
        .removeResources(0)
        .addResources(0, revocableCPU)
        .build();

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, withRevocable);
    checkTaskResources(TASK.getTask(), task);
    assertTrue(task.getResourcesList().stream().anyMatch(Resource::hasRevocable));
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testCreateFromPortsUnset() {
    AssignedTask builder = TASK.newBuilder();
    builder.unsetAssignedPorts();
    builder.setTask(
        resetPorts(ITaskConfig.build(builder.getTask()), ImmutableSet.of()).newBuilder());
    IAssignedTask assignedTask = IAssignedTask.build(builder);
    expect(tierManager.getTier(assignedTask.getTask())).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(IAssignedTask.build(builder), OFFER_THERMOS_EXECUTOR);
    checkTaskResources(ITaskConfig.build(builder.getTask()), task);
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testExecutorInfoNoOverhead() {
    // Here the ram required for the executor is greater than the sum of task resources
    // + executor overhead. We need to ensure we allocate a non-zero amount of ram in this case.
    config = NO_OVERHEAD_EXECUTOR;
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR);
    assertEquals(
        purgeZeroResources(populateDynamicFields(
            NO_OVERHEAD_EXECUTOR.getExecutorConfig().getExecutor(), TASK)),
        task.getExecutor());

    // Simulate the upsizing needed for the task to meet the minimum thermos requirements.
    TaskConfig dummyTask = TASK.getTask().newBuilder();
    checkTaskResources(ITaskConfig.build(dummyTask), task);
    checkDiscoveryInfoUnset(task);
  }

  private void checkTaskResources(ITaskConfig task, TaskInfo taskInfo) {
    assertEquals(
        ResourceSlot.from(task).add(config.getExecutorOverhead()),
        getTotalTaskResources(taskInfo));
  }

  private void checkDiscoveryInfoUnset(TaskInfo taskInfo) {
    assertFalse(taskInfo.hasDiscovery());
  }

  private void checkDiscoveryInfo(
      TaskInfo taskInfo,
      Map<String, Integer> assignedPorts,
      IJobKey job) {

    assertTrue(taskInfo.hasDiscovery());
    Protos.DiscoveryInfo.Builder expectedDiscoveryInfo = Protos.DiscoveryInfo.newBuilder()
        .setVisibility(Protos.DiscoveryInfo.Visibility.CLUSTER)
        .setLocation(CLUSTER_NAME)
        .setEnvironment(job.getEnvironment())
        .setName(getInverseJobSourceName(job));
    for (Map.Entry<String, Integer> entry : assignedPorts.entrySet()) {
      expectedDiscoveryInfo.getPortsBuilder().addPorts(
          Protos.Port.newBuilder()
              .setName(entry.getKey())
              .setProtocol(DEFAULT_PORT_PROTOCOL)
              .setNumber(entry.getValue()));
    }

    assertEquals(expectedDiscoveryInfo.build(), taskInfo.getDiscovery());
  }

  private TaskInfo getDockerTaskInfo() {
    return getDockerTaskInfo(TASK_WITH_DOCKER);
  }

  private TaskInfo getDockerTaskInfo(IAssignedTask task) {
    config = SOME_OVERHEAD_EXECUTOR;

    expect(tierManager.getTier(task.getTask())).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();

    return taskFactory.createFrom(task, OFFER_SOME_OVERHEAD_EXECUTOR);
  }

  @Test
  public void testDockerContainer() {
    DockerInfo docker = getDockerTaskInfo().getExecutor().getContainer().getDocker();
    assertEquals("testimage", docker.getImage());
    assertTrue(docker.getParametersList().isEmpty());
  }

  @Test
  public void testDockerContainerWithParameters() {
    DockerInfo docker = getDockerTaskInfo(TASK_WITH_DOCKER_PARAMS).getExecutor().getContainer()
            .getDocker();
    Parameter parameters = Parameter.newBuilder().setKey("label").setValue("testparameter").build();
    assertEquals(ImmutableList.of(parameters), docker.getParametersList());
  }

  @Test
  public void testGlobalMounts() {
    config = new ExecutorSettings(
        new ExecutorConfig(
            TestExecutorSettings.THERMOS_EXECUTOR_INFO,
            ImmutableList.of(
                Volume.newBuilder()
                    .setHostPath("/host")
                    .setContainerPath("/container")
                    .setMode(Mode.RO)
                    .build())),
        false);

    expect(tierManager.getTier(TASK_WITH_DOCKER.getTask())).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();

    TaskInfo taskInfo = taskFactory.createFrom(TASK_WITH_DOCKER, OFFER_THERMOS_EXECUTOR);
    assertEquals(
        config.getExecutorConfig().getVolumeMounts(),
        taskInfo.getExecutor().getContainer().getVolumesList());
  }

  @Test
  public void testMetadataLabelMapping() {
    expect(tierManager.getTier(TASK.getTask())).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR);
    ImmutableSet<String> labels = task.getLabels().getLabelsList().stream()
        .map(l -> l.getKey() + l.getValue())
        .collect(GuavaUtils.toImmutableSet());

    ImmutableSet<String> metadata = TASK.getTask().getMetadata().stream()
        .map(m -> METADATA_LABEL_PREFIX + m.getKey() + m.getValue())
        .collect(GuavaUtils.toImmutableSet());

    assertEquals(labels, metadata);
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testDockerTaskWithoutExecutor() {
    AssignedTask builder = TASK.newBuilder();
    builder.getTask()
        .setContainer(Container.docker(new DockerContainer()
            .setImage("hello-world")))
        .unsetExecutorConfig();

    TaskInfo task = getDockerTaskInfo(IAssignedTask.build(builder));
    assertTrue(task.hasCommand());
    assertFalse(task.getCommand().getShell());
    assertFalse(task.hasData());
    ContainerInfo expectedContainer = ContainerInfo.newBuilder()
        .setType(Type.DOCKER)
        .setDocker(DockerInfo.newBuilder()
            .setImage("hello-world"))
        .build();
    assertEquals(expectedContainer, task.getContainer());
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testPopulateDiscoveryInfoNoPort() {
    config = new ExecutorSettings(THERMOS_CONFIG, true);
    AssignedTask builder = TASK.newBuilder();
    builder.unsetAssignedPorts();
    builder.setTask(
        resetPorts(ITaskConfig.build(builder.getTask()), ImmutableSet.of()).newBuilder());
    IAssignedTask assignedTask = IAssignedTask.build(builder);
    expect(tierManager.getTier(assignedTask.getTask())).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(IAssignedTask.build(builder), OFFER_THERMOS_EXECUTOR);
    checkTaskResources(ITaskConfig.build(builder.getTask()), task);
    checkDiscoveryInfo(task, ImmutableMap.of(), assignedTask.getTask().getJob());
  }

  @Test
  public void testPopulateDiscoveryInfo() {
    config = new ExecutorSettings(THERMOS_CONFIG, true);
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEV_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager, SERVER_INFO);

    control.replay();
    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR);
    checkTaskResources(TASK.getTask(), task);
    checkDiscoveryInfo(task, ImmutableMap.of("http", 80), TASK.getTask().getJob());
  }

  private static ResourceSlot getTotalTaskResources(TaskInfo task) {
    Resources taskResources = fromResourceList(task.getResourcesList());
    Resources executorResources = fromResourceList(task.getExecutor().getResourcesList());
    return taskResources.slot().add(executorResources.slot());
  }

  private static Resources fromResourceList(Iterable<Resource> resources) {
    return Resources.from(Protos.Offer.newBuilder()
        .setId(Protos.OfferID.newBuilder().setValue("ignored"))
        .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("ignored"))
        .setSlaveId(SlaveID.newBuilder().setValue("ignored"))
        .setHostname("ignored")
        .addAllResources(resources).build());
  }
}
