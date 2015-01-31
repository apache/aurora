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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.DockerContainer;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.MesosContainer;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.scheduler.ResourceSlot.MIN_THERMOS_RESOURCES;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.NO_OVERHEAD_EXECUTOR;
import static org.apache.aurora.scheduler.mesos.TaskExecutors.SOME_OVERHEAD_EXECUTOR;
import static org.junit.Assert.assertEquals;

public class MesosTaskFactoryImplTest {

  private static final String EXECUTOR_WRAPPER_PATH = "/fake/executor_wrapper.sh";
  private static final IAssignedTask TASK = IAssignedTask.build(new AssignedTask()
    .setInstanceId(2)
    .setTaskId("task-id")
    .setAssignedPorts(ImmutableMap.of("http", 80))
    .setTask(new TaskConfig()
        .setJob(new JobKey("role", "environment", "job-name"))
        .setOwner(new Identity("role", "user"))
        .setEnvironment("environment")
        .setJobName("job-name")
        .setDiskMb(10)
        .setRamMb(100)
        .setNumCpus(5)
        .setContainer(Container.mesos(new MesosContainer()))
        .setRequestedPorts(ImmutableSet.of("http"))));
  private static final IAssignedTask TASK_WITH_DOCKER = IAssignedTask.build(TASK.newBuilder()
      .setTask(
          new TaskConfig(TASK.getTask().newBuilder())
              .setContainer(Container.docker(new DockerContainer("testimage")))));

  private static final SlaveID SLAVE = SlaveID.newBuilder().setValue("slave-id").build();

  private MesosTaskFactory taskFactory;
  private ExecutorSettings config;

  private static final ExecutorInfo DEFAULT_EXECUTOR = ExecutorInfo.newBuilder()
      .setExecutorId(MesosTaskFactoryImpl.getExecutorId(TASK.getTaskId()))
      .setName(MesosTaskFactoryImpl.EXECUTOR_NAME)
      .setSource(MesosTaskFactoryImpl.getInstanceSourceName(TASK.getTask(), TASK.getInstanceId()))
      .addAllResources(MesosTaskFactoryImpl.RESOURCES_EPSILON.toResourceList())
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
  }

  @Test
  public void testExecutorInfoUnchanged() {
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());
    checkTaskResources(TASK.getTask(), task);
  }

  @Test
  public void testCreateFromPortsUnset() {
    taskFactory = new MesosTaskFactoryImpl(config);
    AssignedTask assignedTask = TASK.newBuilder();
    assignedTask.getTask().unsetRequestedPorts();
    assignedTask.unsetAssignedPorts();
    TaskInfo task = taskFactory.createFrom(IAssignedTask.build(assignedTask), SLAVE);
    checkTaskResources(ITaskConfig.build(assignedTask.getTask()), task);
  }

  @Test
  public void testExecutorInfoNoOverhead() {
    // Here the ram required for the executor is greater than the sum of task resources
    // + executor overhead. We need to ensure we allocate a non-zero amount of ram in this case.
    config = NO_OVERHEAD_EXECUTOR;
    taskFactory = new MesosTaskFactoryImpl(config);
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
    taskFactory = new MesosTaskFactoryImpl(config);

    AssignedTask builder = TASK.newBuilder();
    builder.getTask()
        .setNumCpus(0.001)
        .setRamMb(1)
        .setDiskMb(0)
        .setRequestedPorts(ImmutableSet.<String>of());
    IAssignedTask assignedTask =
        IAssignedTask.build(builder.setAssignedPorts(ImmutableMap.<String, Integer>of()));

    assertEquals(
        MIN_THERMOS_RESOURCES,
        getTotalTaskResources(taskFactory.createFrom(assignedTask, SLAVE)));
  }

  private void checkTaskResources(ITaskConfig task, TaskInfo taskInfo) {
    assertEquals(
        Resources.sum(Resources.from(task), config.getExecutorOverhead()),
        getTotalTaskResources(taskInfo));
  }

  private TaskInfo getDockerTaskInfo() {
    config = TaskExecutors.SOME_OVERHEAD_EXECUTOR;
    taskFactory = new MesosTaskFactoryImpl(config);
    return taskFactory.createFrom(TASK_WITH_DOCKER, SLAVE);
  }

  @Test
  public void testDockerContainer() {
    TaskInfo task = getDockerTaskInfo();
    assertEquals("testimage", task.getExecutor().getContainer().getDocker().getImage());
  }

  @Test(expected = NullPointerException.class)
  public void testInvalidExecutorSettings() {
    new ExecutorSettings(
        null,
        ImmutableList.<String>of(),
        "",
        Optional.<String>absent(),
        Resources.NONE);
  }

  @Test
  public void testExecutorAndWrapper() {
    config = new ExecutorSettings(
        EXECUTOR_WRAPPER_PATH,
        ImmutableList.of(SOME_OVERHEAD_EXECUTOR.getExecutorPath()),
        "/var/run/thermos",
        Optional.<String>absent(),
        SOME_OVERHEAD_EXECUTOR.getExecutorOverhead());
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo taskInfo = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(EXECUTOR_WITH_WRAPPER, taskInfo.getExecutor());
  }

  private static Resources getTotalTaskResources(TaskInfo task) {
    Resources taskResources = Resources.from(task.getResourcesList());
    Resources executorResources = Resources.from(task.getExecutor().getResourcesList());
    return Resources.sum(taskResources, executorResources);
  }
}
