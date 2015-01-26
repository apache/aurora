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
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import org.apache.aurora.Protobufs;
import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.base.CommandUtil;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.SchedulerException;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.configuration.Resources;
import org.apache.aurora.scheduler.storage.entities.IAssignedTask;
import org.apache.aurora.scheduler.storage.entities.IDockerContainer;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ContainerInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Volume;

import static java.util.Objects.requireNonNull;

/**
 * A factory to create mesos task objects.
 */
public interface MesosTaskFactory {

  /**
   * Creates a mesos task object.
   *
   * @param task Assigned task to translate into a task object.
   * @param slaveId Id of the slave the task is being assigned to.
   * @return A new task.
   * @throws SchedulerException If the task could not be encoded.
   */
  TaskInfo createFrom(IAssignedTask task, SlaveID slaveId) throws SchedulerException;

  // TODO(wfarner): Move this class to its own file to reduce visibility to package private.
  class MesosTaskFactoryImpl implements MesosTaskFactory {
    private static final Logger LOG = Logger.getLogger(MesosTaskFactoryImpl.class.getName());
    private static final String EXECUTOR_PREFIX = "thermos-";

    /**
     * Name to associate with task executors.
     */
    @VisibleForTesting
    static final String EXECUTOR_NAME = "aurora.task";

    private final ExecutorSettings executorSettings;

    @Inject
    MesosTaskFactoryImpl(ExecutorSettings executorSettings) {
      this.executorSettings = requireNonNull(executorSettings);
    }

    @VisibleForTesting
    static ExecutorID getExecutorId(String taskId) {
      return ExecutorID.newBuilder().setValue(EXECUTOR_PREFIX + taskId).build();
    }

    private static String getJobSourceName(IJobKey jobkey) {
      return String.format("%s.%s.%s", jobkey.getRole(), jobkey.getEnvironment(), jobkey.getName());
    }

    private static String getJobSourceName(ITaskConfig task) {
      return getJobSourceName(task.getJob());
    }

    @VisibleForTesting
    static String getInstanceSourceName(ITaskConfig task, int instanceId) {
      return String.format("%s.%s", getJobSourceName(task), instanceId);
    }

    /**
     * Resources to 'allocate' to the executor in the ExecutorInfo.  We do this since mesos
     * disallows an executor with zero resources, but the tasks end up in the same container
     * anyway.
     */
    @VisibleForTesting
    static final Resources RESOURCES_EPSILON = new Resources(
        0.01,
        Amount.of(1L, Data.MB),
        Amount.of(1L, Data.MB),
        0);

    @Override
    public TaskInfo createFrom(IAssignedTask task, SlaveID slaveId) throws SchedulerException {
      requireNonNull(task);
      requireNonNull(slaveId);

      byte[] taskInBytes;
      try {
        taskInBytes = ThriftBinaryCodec.encode(task.newBuilder());
      } catch (ThriftBinaryCodec.CodingException e) {
        LOG.log(Level.SEVERE, "Unable to serialize task.", e);
        throw new SchedulerException("Internal error.", e);
      }

      ITaskConfig config = task.getTask();
      ResourceSlot resourceSlot =
          ResourceSlot.subtract(ResourceSlot.from(config, executorSettings), RESOURCES_EPSILON);

      // TODO(wfarner): Re-evaluate if/why we need to continue handling unset assignedPorts field.
      List<Resource> resources = resourceSlot
          .toResourceList(task.isSetAssignedPorts()
              ? ImmutableSet.copyOf(task.getAssignedPorts().values())
              : ImmutableSet.<Integer>of());

      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine("Setting task resources to "
            + Iterables.transform(resources, Protobufs.SHORT_TOSTRING));
      }
      TaskInfo.Builder taskBuilder =
          TaskInfo.newBuilder()
              .setName(JobKeys.canonicalString(Tasks.ASSIGNED_TO_JOB_KEY.apply(task)))
              .setTaskId(TaskID.newBuilder().setValue(task.getTaskId()))
              .setSlaveId(slaveId)
              .addAllResources(resources)
              .setData(ByteString.copyFrom(taskInBytes));

      if (config.getContainer().isSetMesos()) {
        configureTaskForNoContainer(task, config, taskBuilder);
      } else if (config.getContainer().isSetDocker()) {
        configureTaskForDockerContainer(task, config, taskBuilder);
      } else {
        throw new SchedulerException("Task had no supported container set.");
      }

      return taskBuilder.build();
    }

    private void configureTaskForNoContainer(
        IAssignedTask task,
        ITaskConfig config,
        TaskInfo.Builder taskBuilder) {

      CommandInfo commandInfo = CommandUtil.create(
          executorSettings.getExecutorPath(),
          executorSettings.getExecutorResources(),
          "./",
          executorSettings.getExecutorFlags()).build();

      ExecutorInfo.Builder executorBuilder = configureTaskForExecutor(task, config, commandInfo);
      taskBuilder.setExecutor(executorBuilder.build());
    }

    private void configureTaskForDockerContainer(
        IAssignedTask task,
        ITaskConfig taskConfig,
        TaskInfo.Builder taskBuilder) {

      IDockerContainer config = taskConfig.getContainer().getDocker();
      ContainerInfo.DockerInfo.Builder dockerBuilder = ContainerInfo.DockerInfo.newBuilder()
          .setImage(config.getImage());

      ContainerInfo.Builder containerBuilder = ContainerInfo.newBuilder()
          .setType(ContainerInfo.Type.DOCKER)
          .setDocker(dockerBuilder.build());

      configureContainerVolumes(containerBuilder);

      // TODO(SteveNiemitz): Allow users to specify an executor per container type.
      CommandInfo.Builder commandInfoBuilder = CommandUtil.create(
          executorSettings.getExecutorPath(),
          executorSettings.getExecutorResources(),
          "$MESOS_SANDBOX/",
          executorSettings.getExecutorFlags());

      ExecutorInfo.Builder execBuilder =
          configureTaskForExecutor(task, taskConfig, commandInfoBuilder.build())
              .setContainer(containerBuilder.build());

      taskBuilder.setExecutor(execBuilder.build());
    }

    private ExecutorInfo.Builder configureTaskForExecutor(
        IAssignedTask task,
        ITaskConfig config,
        CommandInfo commandInfo) {

      return ExecutorInfo.newBuilder()
          .setCommand(commandInfo)
          .setExecutorId(getExecutorId(task.getTaskId()))
          .setName(EXECUTOR_NAME)
          .setSource(getInstanceSourceName(config, task.getInstanceId()))
          .addAllResources(RESOURCES_EPSILON.toResourceList());
    }

    private void configureContainerVolumes(ContainerInfo.Builder containerBuilder) {
      containerBuilder.addVolumes(
          Volume.newBuilder()
              .setContainerPath(executorSettings.getThermosObserverRoot())
              .setHostPath(executorSettings.getThermosObserverRoot())
              .setMode(Volume.Mode.RW)
              .build());
    }
  }
}
