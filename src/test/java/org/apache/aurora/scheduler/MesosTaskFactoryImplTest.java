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
package org.apache.aurora.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.quantity.Data;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.MesosTaskFactory.ExecutorConfig;
import org.apache.aurora.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

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

  private MesosTaskFactory taskFactory;

  @Before
  public void setUp() {
    taskFactory = new MesosTaskFactoryImpl(new ExecutorConfig(EXECUTOR_PATH));
  }

  private static final ExecutorInfo DEFAULT_EXECUTOR = ExecutorInfo.newBuilder()
      .setExecutorId(MesosTaskFactoryImpl.getExecutorId(TASK.getTaskId()))
      .setName(MesosTaskFactoryImpl.EXECUTOR_NAME)
      .setSource(MesosTaskFactoryImpl.getInstanceSourceName(TASK.getTask(), TASK.getInstanceId()))
      .addResources(Resources.makeMesosResource(Resources.CPUS, ResourceSlot.EXECUTOR_CPUS.get()))
      .addResources(Resources.makeMesosResource(
          Resources.RAM_MB,
          ResourceSlot.EXECUTOR_RAM.get().as(Data.MB)))
      .setCommand(CommandInfo.newBuilder()
          .setValue("./executor.sh")
          .addUris(URI.newBuilder().setValue(EXECUTOR_PATH).setExecutable(true)))
      .build();

  @Test
  public void testExecutorInfoUnchanged() {
    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);
    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());
    assertEquals(ImmutableSet.of(
            Resources.makeMesosResource(Resources.CPUS, TASK.getTask().getNumCpus()),
            Resources.makeMesosResource(Resources.RAM_MB, TASK.getTask().getRamMb()),
            Resources.makeMesosResource(Resources.DISK_MB, TASK.getTask().getDiskMb()),
            Resources.makeMesosRangeResource(
                Resources.PORTS,
                ImmutableSet.copyOf(TASK.getAssignedPorts().values()))
        ),
        ImmutableSet.copyOf(task.getResourcesList()));
  }

  @Test
  public void testCreateFromPortsUnset() {
    AssignedTask assignedTask = TASK.newBuilder();
    assignedTask.unsetAssignedPorts();
    TaskInfo task = taskFactory.createFrom(IAssignedTask.build(assignedTask), SLAVE);
    assertEquals(DEFAULT_EXECUTOR, task.getExecutor());
    assertEquals(ImmutableSet.of(
            Resources.makeMesosResource(Resources.CPUS, TASK.getTask().getNumCpus()),
            Resources.makeMesosResource(Resources.RAM_MB, TASK.getTask().getRamMb()),
            Resources.makeMesosResource(Resources.DISK_MB, TASK.getTask().getDiskMb())
        ),
        ImmutableSet.copyOf(task.getResourcesList()));
  }
}
