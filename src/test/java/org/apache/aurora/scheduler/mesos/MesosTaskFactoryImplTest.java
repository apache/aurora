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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.ExecutorSettings;
import org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Test;

import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.MIN_TASK_RESOURCES;
import static org.apache.aurora.scheduler.mesos.MesosTaskFactory.MesosTaskFactoryImpl.MIN_THERMOS_RESOURCES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MesosTaskFactoryImplTest {

  private static final String EXECUTOR_PATH = "/twitter/fake/executor.sh";
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
          .setRequestedPorts(ImmutableSet.of("http"))));
  private static final SlaveID SLAVE = SlaveID.newBuilder().setValue("slave-id").build();
  private static final Resources SOME_EXECUTOR_OVERHEAD = new Resources(
      0.01,
      Amount.of(128L, Data.MB),
      Amount.of(0L, Data.MB),
      0);

  private static final Resources NO_EXECUTOR_OVERHEAD = new Resources(
      0,
      Amount.of(0L, Data.MB),
      Amount.of(0L, Data.MB),
      0);

  private MesosTaskFactory taskFactory;
  private ExecutorSettings config;

  private static final ExecutorInfo DEFAULT_EXECUTOR = ExecutorInfo.newBuilder()
      .setExecutorId(MesosTaskFactoryImpl.getExecutorId(TASK.getTaskId()))
      .setName(MesosTaskFactoryImpl.EXECUTOR_NAME)
      .setSource(MesosTaskFactoryImpl.getInstanceSourceName(TASK.getTask(), TASK.getInstanceId()))
      .addAllResources(MIN_THERMOS_RESOURCES.toResourceList())
      .setCommand(CommandInfo.newBuilder()
          .setValue("./executor.sh")
          .addUris(URI.newBuilder().setValue(EXECUTOR_PATH).setExecutable(true)))
      .build();

  @Test
  public void testExecutorInfoUnchanged() {
    config = new ExecutorSettings(EXECUTOR_PATH, SOME_EXECUTOR_OVERHEAD);
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());

    double taskCPU = config.getExecutorOverhead().getNumCpus()
        + TASK.getTask().getNumCpus()
        - MIN_THERMOS_RESOURCES.getNumCpus();
    long taskRamMB = config.getExecutorOverhead().getRam().as(Data.MB)
        + TASK.getTask().getRamMb()
        - MIN_THERMOS_RESOURCES.getRam().as(Data.MB);
    long taskDiskMB = config.getExecutorOverhead().getDisk().as(Data.MB)
        + TASK.getTask().getDiskMb()
        - MIN_THERMOS_RESOURCES.getDisk().as(Data.MB);

    assertTrue(taskCPU > 0.0);
    assertTrue(taskRamMB > 0);
    assertTrue(taskDiskMB > 0);

    assertEquals(ImmutableSet.of(
            Resources.makeMesosResource(Resources.CPUS, taskCPU),
            Resources.makeMesosResource(Resources.DISK_MB, taskDiskMB),
            Resources.makeMesosResource(Resources.RAM_MB, taskRamMB),
            Resources.makeMesosRangeResource(
                Resources.PORTS,
                ImmutableSet.copyOf(TASK.getAssignedPorts().values()))
        ),
        ImmutableSet.copyOf(task.getResourcesList()));
  }

  @Test
  public void testCreateFromPortsUnset() {
    config = new ExecutorSettings(EXECUTOR_PATH, SOME_EXECUTOR_OVERHEAD);
    taskFactory = new MesosTaskFactoryImpl(config);
    AssignedTask assignedTask = TASK.newBuilder();
    assignedTask.unsetAssignedPorts();
    TaskInfo task = taskFactory.createFrom(IAssignedTask.build(assignedTask), SLAVE);

    double taskCPU = config.getExecutorOverhead().getNumCpus()
        + TASK.getTask().getNumCpus()
        - MIN_THERMOS_RESOURCES.getNumCpus();
    long taskRamMB = config.getExecutorOverhead().getRam().as(Data.MB)
        + TASK.getTask().getRamMb()
        - MIN_THERMOS_RESOURCES.getRam().as(Data.MB);
    long taskDiskMB = config.getExecutorOverhead().getDisk().as(Data.MB)
        + TASK.getTask().getDiskMb()
        - MIN_THERMOS_RESOURCES.getDisk().as(Data.MB);

    assertTrue(taskCPU > 0.0);
    assertTrue(taskRamMB > 0);
    assertTrue(taskDiskMB > 0);

    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());
    assertEquals(ImmutableSet.of(
            Resources.makeMesosResource(Resources.CPUS, taskCPU),
            Resources.makeMesosResource(Resources.DISK_MB, taskDiskMB),
            Resources.makeMesosResource(Resources.RAM_MB, taskRamMB)
        ),
        ImmutableSet.copyOf(task.getResourcesList()));
  }

  @Test
  public void testExecutorInfoNoOverhead() {
    // Here the ram required for the executor is greater than the sum of task resources
    // + executor overhead. We need to ensure we allocate a non-zero amount of ram in this case.
    config = new ExecutorSettings(EXECUTOR_PATH, NO_EXECUTOR_OVERHEAD);
    taskFactory = new MesosTaskFactoryImpl(config);
    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());

    double taskCPU = config.getExecutorOverhead().getNumCpus()
        + TASK.getTask().getNumCpus()
        - MIN_THERMOS_RESOURCES.getNumCpus();

    long taskDiskMB = config.getExecutorOverhead().getDisk().as(Data.MB)
        + TASK.getTask().getDiskMb()
        - MIN_THERMOS_RESOURCES.getDisk().as(Data.MB);

    assertTrue(taskCPU > 0.0);
    assertTrue(taskDiskMB > 0);

    assertEquals(ImmutableSet.of(
      Resources.makeMesosResource(Resources.CPUS, taskCPU),
      Resources.makeMesosResource(Resources.RAM_MB, MIN_TASK_RESOURCES.getRam().as(Data.MB)),
      Resources.makeMesosResource(Resources.DISK_MB, taskDiskMB),
      Resources.makeMesosRangeResource(
          Resources.PORTS,
          ImmutableSet.copyOf(TASK.getAssignedPorts().values()))
    ),
    ImmutableSet.copyOf(task.getResourcesList()));
  }

  @Test
  public void testMaxElements() {
    Resources highRAM = new Resources(1, Amount.of(8L, Data.GB), Amount.of(10L, Data.MB), 0);
    Resources rest = new Resources(10, Amount.of(1L, Data.MB), Amount.of(10L, Data.GB), 1);

    Resources result = MesosTaskFactoryImpl.maxElements(highRAM, rest);
    assertEquals(result.getNumCpus(), 10, 0.001);
    assertEquals(result.getRam(), Amount.of(8L, Data.GB));
    assertEquals(result.getDisk(), Amount.of(10L, Data.GB));
    assertEquals(result.getNumPorts(), 1);
  }

}
