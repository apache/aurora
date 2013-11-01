/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value.Scalar;
import org.apache.mesos.Protos.Value.Type;
import org.junit.Before;
import org.junit.Test;

import com.twitter.aurora.gen.AssignedTask;
import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.TaskConfig;
import com.twitter.aurora.scheduler.MesosTaskFactory.ExecutorConfig;
import com.twitter.aurora.scheduler.MesosTaskFactory.MesosTaskFactoryImpl;
import com.twitter.aurora.scheduler.storage.entities.IAssignedTask;
import com.twitter.common.quantity.Data;

import static org.junit.Assert.assertEquals;

public class MesosTaskFactoryImplTest {

  private static final String EXECUTOR_PATH = "/twitter/fake/executor.sh";
  private static final IAssignedTask TASK = IAssignedTask.build(new AssignedTask()
      .setInstanceId(2)
      .setTaskId("task-id")
      .setTask(new TaskConfig()
          .setOwner(new Identity("role", "user"))
          .setEnvironment("environment")
          .setJobName("job-name")
          .setDiskMb(10)
          .setRamMb(100)
          .setNumCpus(5)));
  private static final SlaveID SLAVE = SlaveID.newBuilder().setValue("slave-id").build();

  private MesosTaskFactory taskFactory;

  @Before
  public void setUp() {
    taskFactory = new MesosTaskFactoryImpl(new ExecutorConfig(EXECUTOR_PATH));
  }

  @Test
  public void testExecutorInfoUnchanged() {
    // Tests against regression of MESOS-911.
    TaskInfo task = taskFactory.createFrom(TASK, SLAVE);

    ExecutorInfo expected = ExecutorInfo.newBuilder()
        .setExecutorId(MesosTaskFactoryImpl.getExecutorId(TASK.getTaskId()))
        .setName(MesosTaskFactoryImpl.EXECUTOR_NAME)
        .setSource(MesosTaskFactoryImpl.getInstanceSourceName(TASK.getTask(), TASK.getInstanceId()))
        .addResources(Resource.newBuilder()
            .setName("cpus")
            .setType(Type.SCALAR)
            .setScalar(Scalar.newBuilder().setValue(ResourceSlot.EXECUTOR_CPUS)))
        .addResources(Resource.newBuilder()
            .setName("mem")
            .setType(Type.SCALAR)
            .setScalar(Scalar.newBuilder().setValue(ResourceSlot.EXECUTOR_RAM.as(Data.MB))))
        .setCommand(CommandInfo.newBuilder()
            .setValue("./executor.sh")
            .addUris(URI.newBuilder().setValue(EXECUTOR_PATH).setExecutable(true)))
        .build();

    assertEquals(expected, task.getExecutor());
  }
}
