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

import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.DockerParameter;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.Resources;
import org.apache.aurora.scheduler.TierManager;
import org.apache.aurora.scheduler.configuration.executor.ExecutorConfig;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.ContainerInfo.DockerInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Parameter;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Volume;
import org.apache.mesos.Protos.Volume.Mode;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.TierInfo.DEFAULT;
import static org.apache.aurora.scheduler.base.TaskTestUtil.REVOCABLE_TIER;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TestExecutorSettings.THERMOS_CONFIG;
import static org.apache.aurora.scheduler.mesos.TestExecutorSettings.THERMOS_EXECUTOR;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MesosTaskFactoryImplTest extends EasyMockTest {

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

  @Test
  public void testExecutorInfoUnchanged() {
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEFAULT);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);

    assertEquals(populateDynamicFields(DEFAULT_EXECUTOR, TASK), task.getExecutor());
    checkTaskResources(TASK.getTask(), task);
  }

  @Test
  public void testTaskInfoRevocable() {
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(REVOCABLE_TIER);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    checkTaskResources(TASK.getTask(), task);
    assertTrue(task.getResourcesList().stream().anyMatch(Resource::hasRevocable));
  }

  @Test
  public void testCreateFromPortsUnset() {
    AssignedTask builder = TASK.newBuilder();
    builder.getTask().unsetRequestedPorts();
    builder.unsetAssignedPorts();
    IAssignedTask assignedTask = IAssignedTask.build(builder);
    expect(tierManager.getTier(assignedTask.getTask())).andReturn(DEFAULT);
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
    expect(tierManager.getTier(TASK_CONFIG)).andReturn(DEFAULT);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(
        populateDynamicFields(NO_OVERHEAD_EXECUTOR.getExecutorConfig().getExecutor(), TASK),
        task.getExecutor());

    // Simulate the upsizing needed for the task to meet the minimum thermos requirements.
    TaskConfig dummyTask = TASK.getTask().newBuilder();
    checkTaskResources(ITaskConfig.build(dummyTask), task);
  }

  private void checkTaskResources(ITaskConfig task, TaskInfo taskInfo) {
    assertEquals(
        ResourceSlot.from(task).add(config.getExecutorOverhead()),
        getTotalTaskResources(taskInfo));
  }

  private TaskInfo getDockerTaskInfo() {
    return getDockerTaskInfo(TASK_WITH_DOCKER);
  }

  private TaskInfo getDockerTaskInfo(IAssignedTask task) {
    config = TaskExecutors.SOME_OVERHEAD_EXECUTOR;
    expect(tierManager.getTier(task.getTask())).andReturn(DEFAULT);
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

  @Test
  public void testGlobalMounts() {
    config = new ExecutorSettings(new ExecutorConfig(
        TestExecutorSettings.THERMOS_EXECUTOR_INFO,
        ImmutableList.of(
            Volume.newBuilder()
                .setHostPath("/host")
                .setContainerPath("/container")
                .setMode(Mode.RO)
                .build())));

    expect(tierManager.getTier(TASK_WITH_DOCKER.getTask())).andReturn(DEFAULT);
    taskFactory = new MesosTaskFactoryImpl(config, tierManager);

    control.replay();

    TaskInfo taskInfo = taskFactory.createFrom(TASK_WITH_DOCKER, SLAVE);
    assertEquals(
        config.getExecutorConfig().getVolumeMounts(),
        taskInfo.getExecutor().getContainer().getVolumesList());
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
