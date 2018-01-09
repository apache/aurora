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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AppcImage;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerImage;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.Image;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.executor.ExecutorConfig;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.resources.ResourceBag;
import org.apache.aurora.scheduler.resources.ResourceManager;
import org.apache.aurora.scheduler.resources.ResourceType;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.v1.Protos;
import org.apache.mesos.v1.Protos.AgentID;
import org.apache.mesos.v1.Protos.CommandInfo.URI;
import org.apache.mesos.v1.Protos.ContainerInfo;
import org.apache.mesos.v1.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.v1.Protos.ContainerInfo.MesosInfo;
import org.apache.mesos.v1.Protos.ContainerInfo.Type;
import org.apache.mesos.v1.Protos.ExecutorInfo;
import org.apache.mesos.v1.Protos.Offer;
import org.apache.mesos.v1.Protos.Parameter;
import org.apache.mesos.v1.Protos.Resource;
import org.apache.mesos.v1.Protos.TaskInfo;
import org.apache.mesos.v1.Protos.Volume;
import org.apache.mesos.v1.Protos.Volume.Mode;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.apiConstants.TASK_FILESYSTEM_MOUNT_POINT;
import static org.apache.aurora.scheduler.base.TaskTestUtil.PROD_TIER_NAME;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.DEFAULT_PORT_PROTOCOL;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.METADATA_LABEL_PREFIX;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.SOURCE_LABEL;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.TIER_LABEL;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.getInstanceSourceName;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.getInverseJobSourceName;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.SOME_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TestExecutorSettings.THERMOS_CONFIG;
import static org.apache.aurora.scheduler.mesos.TestExecutorSettings.THERMOS_EXECUTOR;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromMesosResources;
import static org.apache.aurora.scheduler.resources.ResourceManager.bagFromResources;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosRange;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalarFromBag;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.resetPorts;
import static org.apache.aurora.scheduler.resources.ResourceType.PORTS;
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

  private static final ExecutorSettings EXECUTOR_SETTINGS_WITH_VOLUMES = new ExecutorSettings(
      ImmutableMap.<String, ExecutorConfig>builder().
          put(TestExecutorSettings.THERMOS_EXECUTOR_INFO.getName(),
              new ExecutorConfig(
                  TestExecutorSettings.THERMOS_EXECUTOR_INFO,
                  ImmutableList.of(
                      Volume.newBuilder()
                          .setHostPath("/host")
                          .setContainerPath("/container")
                          .setMode(Mode.RO).build()),
                  TestExecutorSettings.THERMOS_TASK_PREFIX)).build(),
      false /* populate discovery info */);

  private static final AgentID SLAVE = AgentID.newBuilder().setValue("slave-id").build();
  private static final Offer OFFER_THERMOS_EXECUTOR = Protos.Offer.newBuilder()
      .setId(Protos.OfferID.newBuilder().setValue("offer-id"))
      .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework-id"))
      .setAgentId(SLAVE)
      .setHostname("slave-hostname")
      .addAllResources(mesosScalarFromBag(bagFromResources(
              TASK_CONFIG.getResources()).add(THERMOS_EXECUTOR.getExecutorOverhead(TASK_CONFIG))))
      .addResources(mesosRange(PORTS, 80))
      .build();
  private static final Offer OFFER_SOME_OVERHEAD_EXECUTOR = OFFER_THERMOS_EXECUTOR.toBuilder()
      .clearResources()
      .addAllResources(mesosScalarFromBag(bagFromResources(
          TASK_CONFIG.getResources()).add(SOME_OVERHEAD_EXECUTOR.getExecutorOverhead(TASK_CONFIG))))
      .addResources(mesosRange(PORTS, 80))
      .build();

  private static final String CLUSTER_NAME = "cluster_name";
  private static final IServerInfo SERVER_INFO = IServerInfo.build(
      new ServerInfo(CLUSTER_NAME, ""));

  private MesosTaskFactory taskFactory;
  private ExecutorSettings config;

  private static final ExecutorInfo DEFAULT_EXECUTOR = THERMOS_CONFIG.getExecutor();

  @Before
  public void setUp() {
    config = THERMOS_EXECUTOR;

    ResourceType.initializeEmptyCliArgsForTest();
  }

  private static ExecutorInfo populateDynamicFields(ExecutorInfo executor, IAssignedTask task) {
    String sourceName = getInstanceSourceName(task.getTask(), task.getInstanceId());
    return executor.toBuilder()
        .clearResources()
        .setExecutorId(MesosTaskFactoryImpl.getExecutorId(
            task.getTaskId(),
            THERMOS_EXECUTOR.getExecutorConfig(executor.getName()).get().getTaskPrefix()))
        .setSource(sourceName)
        .setLabels(
            Protos.Labels.newBuilder().addLabels(
                Protos.Label.newBuilder()
                    .setKey(SOURCE_LABEL)
                    .setValue(sourceName)))
        .setCommand(executor.getCommand().toBuilder().addAllUris(
            ImmutableSet.of(
                URI.newBuilder()
                    .setValue("pathA")
                    .setExecutable(false)
                    .setExtract(true)
                    .setCache(true).build(),
                URI.newBuilder()
                    .setValue("pathB")
                    .setExecutable(false)
                    .setExtract(true)
                    .setCache(true).build())))
        .build();
  }

  private static ExecutorInfo makeComparable(ExecutorInfo executorInfo) {
    return executorInfo.toBuilder().clearResources().build();
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
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR, false);

    assertEquals(populateDynamicFields(DEFAULT_EXECUTOR, TASK), makeComparable(task.getExecutor()));
    checkTaskResources(TASK.getTask(), task);
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testTaskInfoRevocable() {
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    List<Resource> revocable = OFFER_THERMOS_EXECUTOR.getResourcesList().stream()
        .map(r -> {
          ResourceType type = ResourceType.fromResource(r);
          if (type.isMesosRevocable()) {
            r = r.toBuilder().setRevocable(Resource.RevocableInfo.getDefaultInstance()).build();
          }
          return r;
        })
        .collect(Collectors.toList());

    Offer withRevocable = OFFER_THERMOS_EXECUTOR.toBuilder()
        .clearResources()
        .addAllResources(revocable)
        .build();

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, withRevocable, true);
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
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    TaskInfo task =
        taskFactory.createFrom(IAssignedTask.build(builder), OFFER_THERMOS_EXECUTOR, false);
    checkTaskResources(ITaskConfig.build(builder.getTask()), task);
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testExecutorInfoNoOverhead() {
    // Here the ram required for the executor is greater than the sum of task resources
    // + executor overhead. We need to ensure we allocate a non-zero amount of ram in this case.
    config = NO_OVERHEAD_EXECUTOR;
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR, false);
    assertEquals(
        purgeZeroResources(populateDynamicFields(
            NO_OVERHEAD_EXECUTOR.getExecutorConfig(TASK.getTask()
                .getExecutorConfig()
                .getName()).get()
                .getExecutor(),
            TASK)),
        makeComparable(task.getExecutor()));

    // Simulate the upsizing needed for the task to meet the minimum thermos requirements.
    TaskConfig dummyTask = TASK.getTask().newBuilder();
    checkTaskResources(ITaskConfig.build(dummyTask), task);
    checkDiscoveryInfoUnset(task);
  }

  private void checkTaskResources(ITaskConfig task, TaskInfo taskInfo) {
    ResourceBag taskResources = bagFromMesosResources(taskInfo.getResourcesList());
    ResourceBag executorResources =
        bagFromMesosResources(taskInfo.getExecutor().getResourcesList());

    assertEquals(ResourceManager.bagFromTask(task, config), taskResources.add(executorResources));
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
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    return taskFactory.createFrom(task, OFFER_SOME_OVERHEAD_EXECUTOR, false);
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
    config = EXECUTOR_SETTINGS_WITH_VOLUMES;
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    TaskInfo taskInfo = taskFactory.createFrom(TASK_WITH_DOCKER, OFFER_THERMOS_EXECUTOR, false);
    assertEquals(
        config.getExecutorConfig(TASK_WITH_DOCKER.getTask().getExecutorConfig().getName()).get()
            .getVolumeMounts(),
        taskInfo.getExecutor().getContainer().getVolumesList());
  }

  @Test
  public void testContainerVolumes() {
    String imageName = "some-image-name";
    String imageTag = "some-image-tag";
    org.apache.aurora.gen.Volume volume =
        new org.apache.aurora.gen.Volume("container", "/host", org.apache.aurora.gen.Mode.RO);
    IAssignedTask taskWithImageAndVolumes = IAssignedTask.build(TASK.newBuilder()
        .setTask(
            new TaskConfig(TASK.getTask().newBuilder()
                .setContainer(Container.mesos(
                    new MesosContainer()
                        .setImage(Image.docker(new DockerImage(imageName, imageTag)))
                        .setVolumes(ImmutableList.of(volume)))))));

    control.replay();

    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);
    TaskInfo task = taskFactory.createFrom(taskWithImageAndVolumes, OFFER_THERMOS_EXECUTOR, false);

    assertEquals(
        ContainerInfo.newBuilder()
            .setType(Type.MESOS)
            .setMesos(MesosInfo.newBuilder())
            .addVolumes(Volume.newBuilder()
                .setContainerPath("container")
                .setHostPath("/host")
                .setMode(Mode.RO))
            .addVolumes(Volume.newBuilder()
                .setContainerPath(TASK_FILESYSTEM_MOUNT_POINT)
                .setImage(Protos.Image.newBuilder()
                    .setType(Protos.Image.Type.DOCKER)
                    .setDocker(Protos.Image.Docker.newBuilder()
                        .setName(imageName + ":" + imageTag)))
                .setMode(Mode.RO))
            .build(),
        task.getExecutor().getContainer());
  }

  @Test
  public void testMetadataLabelMapping() {
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR, false);
    ImmutableSet<String> labels = task.getLabels().getLabelsList().stream()
        .filter(l -> l.getKey().startsWith(METADATA_LABEL_PREFIX))
        .map(l -> l.getKey() + l.getValue())
        .collect(GuavaUtils.toImmutableSet());

    ImmutableSet<String> metadata = TASK.getTask().getMetadata().stream()
        .map(m -> METADATA_LABEL_PREFIX + m.getKey() + m.getValue())
        .collect(GuavaUtils.toImmutableSet());

    assertEquals(labels, metadata);
    checkDiscoveryInfoUnset(task);
  }

  @Test
  public void testTierLabel() {
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR, false);

    assertTrue(task.getLabels().getLabelsList().stream().anyMatch(
        l -> l.getKey().equals(TIER_LABEL) && l.getValue().equals(PROD_TIER_NAME)));
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
    config = new ExecutorSettings(
        ImmutableMap.<String, ExecutorConfig>builder().put(THERMOS_CONFIG.getExecutor().getName(),
            THERMOS_CONFIG).build(),
        true /* populate discovery info */);
    AssignedTask builder = TASK.newBuilder();
    builder.unsetAssignedPorts();
    builder.setTask(
        resetPorts(ITaskConfig.build(builder.getTask()), ImmutableSet.of()).newBuilder());
    IAssignedTask assignedTask = IAssignedTask.build(builder);
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();

    TaskInfo task =
        taskFactory.createFrom(IAssignedTask.build(builder), OFFER_THERMOS_EXECUTOR, false);
    checkTaskResources(ITaskConfig.build(builder.getTask()), task);
    checkDiscoveryInfo(task, ImmutableMap.of(), assignedTask.getTask().getJob());
  }

  @Test
  public void testPopulateDiscoveryInfo() {
    config = new ExecutorSettings(
        ImmutableMap.<String, ExecutorConfig>builder().put(THERMOS_CONFIG.getExecutor().getName(),
            THERMOS_CONFIG).build(),
        true /* populate discovery info */);
    taskFactory = new MesosTaskFactoryImpl(config, SERVER_INFO);

    control.replay();
    TaskInfo task = taskFactory.createFrom(TASK, OFFER_THERMOS_EXECUTOR, false);
    checkTaskResources(TASK.getTask(), task);
    checkDiscoveryInfo(task, ImmutableMap.of("http", 80), TASK.getTask().getJob());
  }

  @Test
  public void testDockerImageWithMesosContainer() throws Exception {
    String imageName = "some-image-name";
    String imageTag = "some-image-tag";
    IAssignedTask taskWithDockerImage = IAssignedTask.build(TASK.newBuilder()
        .setTask(
            new TaskConfig(TASK.getTask().newBuilder()
                .setContainer(Container.mesos(
                    new MesosContainer().setImage(
                        Image.docker(new DockerImage(imageName, imageTag))))))));

    control.replay();

    taskFactory = new MesosTaskFactoryImpl(EXECUTOR_SETTINGS_WITH_VOLUMES, SERVER_INFO);
    TaskInfo task = taskFactory.createFrom(taskWithDockerImage, OFFER_THERMOS_EXECUTOR, false);
    assertEquals(
        ContainerInfo.newBuilder()
            .setType(Type.MESOS)
            .setMesos(MesosInfo.newBuilder())
            .addAllVolumes(EXECUTOR_SETTINGS_WITH_VOLUMES.getExecutorConfig(
                TASK.getTask().getExecutorConfig().getName()).get().getVolumeMounts())
            .addVolumes(Volume.newBuilder()
                .setContainerPath(TASK_FILESYSTEM_MOUNT_POINT)
                .setImage(Protos.Image.newBuilder()
                    .setType(Protos.Image.Type.DOCKER)
                    .setDocker(Protos.Image.Docker.newBuilder()
                        .setName(imageName + ":" + imageTag)))
                .setMode(Mode.RO))
            .build(),
        task.getExecutor().getContainer());
  }

  @Test
  public void testAppcImageWithMesosContainer() throws Exception {
    String imageName = "some-image-name";
    String imageId = "some-image-id";
    IAssignedTask taskWithAppcImage = IAssignedTask.build(TASK.newBuilder()
        .setTask(
            new TaskConfig(TASK.getTask().newBuilder()
                .setContainer(Container.mesos(
                    new MesosContainer().setImage(
                        Image.appc(new AppcImage(imageName, imageId))))))));

    control.replay();

    taskFactory = new MesosTaskFactoryImpl(EXECUTOR_SETTINGS_WITH_VOLUMES, SERVER_INFO);

    TaskInfo task = taskFactory.createFrom(taskWithAppcImage, OFFER_THERMOS_EXECUTOR, false);
    assertEquals(
        ContainerInfo.newBuilder()
            .setType(Type.MESOS)
            .setMesos(MesosInfo.newBuilder())
            .addAllVolumes(EXECUTOR_SETTINGS_WITH_VOLUMES.getExecutorConfig(
                TASK.getTask().getExecutorConfig().getName()).get().getVolumeMounts())
            .addVolumes(Volume.newBuilder()
                .setContainerPath(TASK_FILESYSTEM_MOUNT_POINT)
                .setImage(Protos.Image.newBuilder()
                    .setType(Protos.Image.Type.APPC)
                    .setAppc(Protos.Image.Appc.newBuilder()
                        .setName(imageName)
                        .setId(imageId)))
                .setMode(Mode.RO))
            .build(),
        task.getExecutor().getContainer());
  }
}
