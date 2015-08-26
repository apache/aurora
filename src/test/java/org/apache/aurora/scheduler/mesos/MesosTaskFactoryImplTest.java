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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Data;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.Mode;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.Volume;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.Resources;
import org.apache.aurora.scheduler.TierInfo;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Parameter;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.ResourceSlot.MIN_THERMOS_RESOURCES;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.RESOURCES_EPSILON;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.SOME_OVERHEAD_EXECUTOR;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MesosTaskFactoryImplTest extends EasyMockTest {

  private static final String EXECUTOR_WRAPPER_PATH = "/fake/executor_wrapper.sh";
  private static final ITaskConfig TASK_CONFIG = ITaskConfig.build(new TaskConfig()
      .setJob(new JobKey("role", "environment", "job-name"))
      .setOwner(new Identity("role", "user"))
      .setEnvironment("environment")
      .setJobName("job-name")
      .setDiskMb(10)
      .setRamMb(100)
      .setNumCpus(5)
      .setContainer(Container.mesos(new MesosContainer()))
      .setRequestedPorts(ImmutableSet.of("http")));
  private static final TierInfo DEFAULT_TIER = new TierInfo(false);
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

  private MesosTaskFactory taskFactory;
  private ExecutorSettings config;
  private TierManager tierManager;

  private static final ExecutorInfo DEFAULT_EXECUTOR = ExecutorInfo.newBuilder()
      .setExecutorId(MesosTaskFactoryImpl.getExecutorId(TASK.getTaskId()))
      .setName(MesosTaskFactoryImpl.EXECUTOR_NAME)
      .setSource(MesosTaskFactoryImpl.getInstanceSourceName(TASK.getTask(), TASK.getInstanceId()))
      .addAllResources(RESOURCES_EPSILON.toResourceList(DEFAULT_TIER))
      .setCommand(CommandInfo.newBuilder()
          .setValue("./executor.pex")
          .addUris(URI.newBuilder().setValue(NO_OVERHEAD_EXECUTOR.getExecutorPath())
              .setExecutable(true)))
      .build();

  private static final ExecutorInfo EXECUTOR_WITH_WRAPPER =
      ExecutorInfo.newBuilder(DEFAULT_EXECUTOR)
          .setCommand(CommandInfo.newBuilder()
              .setValue("./executor_wrapper.sh")
              .addUris(URI.newBuilder().setValue(NO_OVERHEAD_EXECUTOR.getExecutorPath())
                  .setExecutable(true))
              .addUris(URI.newBuilder().setValue(EXECUTOR_WRAPPER_PATH).setExecutable(true)))
          .build();

  @Before
  public void setUp() {
    config = TaskExecutors.SOME_OVERHEAD_EXECUTOR;
    tierManager = createMock(TierManager.class);
  }

  @Test
  public void testExecutorInfoUnchanged() {
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEFAULT_TIER).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());
    checkTaskResources(TASK.getTask(), task);
  }

  @Test
  public void testTaskInfoRevocable() {
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(new TierInfo(true)).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    checkTaskResources(TASK.getTask(), task);
    assertTrue(task.getExecutor().getResourcesList().stream().anyMatch(Resource::hasRevocable));
    assertTrue(task.getResourcesList().stream().anyMatch(Resource::hasRevocable));
  }

  @Test
  public void testCreateFromPortsUnset() {
    AssignedTask builder = TASK.newBuilder();
    builder.getTask().unsetRequestedPorts();
    builder.unsetAssignedPorts();
    IAssignedTask assignedTask = IAssignedTask.build(builder);
    expect(tierManager.getTier(assignedTask.getTask())).andReturn(DEFAULT_TIER).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo task = taskFactory.createFrom(IAssignedTask.build(builder), SLAVE);
    checkTaskResources(ITaskConfig.build(builder.getTask()), task);
  }

  @Test
  public void testExecutorInfoNoOverhead() {
    // Here the ram required for the executor is greater than the sum of task resources
    // + executor overhead. We need to ensure we allocate a non-zero amount of ram in this case.
    config = NO_OVERHEAD_EXECUTOR;
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEFAULT_TIER).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());

    // Simulate the upsizing needed for the task to meet the minimum thermos requirements.
    TaskConfig dummyTask = TASK.getTask().newBuilder()
        .setRamMb(ResourceSlot.MIN_THERMOS_RESOURCES.getRam().as(Data.MB));
    checkTaskResources(ITaskConfig.build(dummyTask), task);
  }

  @Test
  public void testSmallTaskUpsizing() {
    // A very small task should be upsized to support the minimum resources required by the
    // executor.

    config = NO_OVERHEAD_EXECUTOR;
    AssignedTask builder = TASK.newBuilder();
    builder.getTask()
        .setNumCpus(0.001)
        .setRamMb(1)
        .setDiskMb(0)
        .setRequestedPorts(ImmutableSet.of());
    IAssignedTask assignedTask =
        IAssignedTask.build(builder.setAssignedPorts(ImmutableMap.of()));
    expect(tierManager.getTier(assignedTask.getTask())).andReturn(DEFAULT_TIER).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    assertEquals(
        MIN_THERMOS_RESOURCES,
        getTotalTaskResources(taskFactory.createFrom(assignedTask, SLAVE)));
  }

  private void checkTaskResources(ITaskConfig task, TaskInfo taskInfo) {
    assertEquals(
        ResourceSlot.from(task).withOverhead(config),
        getTotalTaskResources(taskInfo));
  }

  private TaskInfo getDockerTaskInfo() {
    return getDockerTaskInfo(TASK_WITH_DOCKER);
  }

  private TaskInfo getDockerTaskInfo(IAssignedTask task) {
    config = TaskExecutors.SOME_OVERHEAD_EXECUTOR;
    expect(tierManager.getTier(task.getTask())).andReturn(DEFAULT_TIER).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    return taskFactory.createFrom(task, SLAVE);
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

  @Test(expected = NullPointerException.class)
  public void testInvalidExecutorSettings() {
    control.replay();

    ExecutorSettings.newBuilder()
        .setExecutorPath(null)
        .setThermosObserverRoot("")
        .build();
  }

  @Test
  public void testExecutorAndWrapper() {
    config = ExecutorSettings.newBuilder()
        .setExecutorPath(EXECUTOR_WRAPPER_PATH)
        .setExecutorResources(ImmutableList.of(SOME_OVERHEAD_EXECUTOR.getExecutorPath()))
        .setThermosObserverRoot("/var/run/thermos")
        .setExecutorOverhead(SOME_OVERHEAD_EXECUTOR.getExecutorOverhead())
        .build();
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEFAULT_TIER).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo taskInfo = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(EXECUTOR_WITH_WRAPPER, taskInfo.getExecutor());
  }

  @Test
  public void testGlobalMounts() {
    config = ExecutorSettings.newBuilder()
        .setExecutorPath(EXECUTOR_WRAPPER_PATH)
        .setExecutorResources(ImmutableList.of(SOME_OVERHEAD_EXECUTOR.getExecutorPath()))
        .setThermosObserverRoot("/var/run/thermos")
        .setExecutorOverhead(SOME_OVERHEAD_EXECUTOR.getExecutorOverhead())
        .setGlobalContainerMounts(ImmutableList.of(new Volume("/container", "/host", Mode.RO)))
        .build();
    expect(tierManager.getTier(TASK_WITH_DOCKER.getTask())).andReturn(DEFAULT_TIER).times(2);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo taskInfo = taskFactory.createFrom(TASK_WITH_DOCKER, SLAVE);
    Protos.Volume expected = Protos.Volume.newBuilder()
        .setHostPath("/host")
        .setContainerPath("/container")
        .setMode(Protos.Volume.Mode.RO)
        .build();
    assertTrue(taskInfo.getExecutor().getContainer().getVolumesList().contains(expected));
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
