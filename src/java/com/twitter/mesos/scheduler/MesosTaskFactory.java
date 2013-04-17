package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.Protobufs;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.configuration.Resources;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

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
   * @throws com.twitter.mesos.scheduler.SchedulerException If the task could not be encoded.
   */
  TaskInfo createFrom(AssignedTask task, SlaveID slaveId) throws SchedulerException;

  static class MesosTaskFactoryImpl implements MesosTaskFactory {
    private static final Logger LOG = Logger.getLogger(MesosTaskFactoryImpl.class.getName());

    /**
     * Name to associate with task executors.
     */
    @VisibleForTesting
    static final String EXECUTOR_NAME = "aurora.task";

    /**
     * CPU allocated for each executor.  TODO(wickman) Consider lowing this number if sufficient.
     */
    @VisibleForTesting
    static final double CPUS = 0.25;

    /**
     * RAM required for the executor.  Executors in the wild have been observed using 48-54MB RSS,
     * setting to 128MB to be extra vigilant initially.
     */
    @VisibleForTesting
    static final Amount<Long, Data> RAM = Amount.of(128L, Data.MB);

    /**
     * Return the total CPU consumption of this task including the executor.
     *
     * @param taskCpus Number of CPUs required for the user task.
     */
    public static double getTotalTaskCpus(double taskCpus) {
      return taskCpus + CPUS;
    }

    /**
     * Return the total RAM consumption of this task including the executor.
     *
     * @param taskRamMb Memory requirement for the user task, in megabytes.
     */
    public static Amount<Long, Data> getTotalTaskRam(long taskRamMb) {
      return Amount.of(taskRamMb + RAM.as(Data.MB), Data.MB);
    }

    static class ExecutorConfig {
      private final String executorPath;

      ExecutorConfig(String executorPath) {
        this.executorPath = checkNotBlank(executorPath);
      }

      String getExecutorPath() {
        return executorPath;
      }
    }

    private final String executorPath;

    @Inject
    MesosTaskFactoryImpl(ExecutorConfig executorConfig) {
      this.executorPath = executorConfig.getExecutorPath();
    }

    @VisibleForTesting
    static ExecutorID getExecutorId(String taskId) {
      return ExecutorID.newBuilder().setValue("thermos-" + taskId).build();
    }

    @VisibleForTesting
    static String getSourceName(TwitterTaskInfo task) {
      return String.format("%s.%s.%s.%s",
          task.getOwner().getRole(), task.getEnvironment(), task.getJobName(), task.getShardId());
    }

    @Override
    public TaskInfo createFrom(AssignedTask task, SlaveID slaveId) throws SchedulerException {
      checkNotNull(task);
      byte[] taskInBytes;
      try {
        taskInBytes = ThriftBinaryCodec.encode(task);
      } catch (ThriftBinaryCodec.CodingException e) {
        LOG.log(Level.SEVERE, "Unable to serialize task.", e);
        throw new SchedulerException("Internal error.", e);
      }

      TwitterTaskInfo config = task.getTask();
      List<Resource> resources;
      if (task.isSetAssignedPorts()) {
        resources = Resources.from(config)
            .toResourceList(ImmutableSet.copyOf(task.getAssignedPorts().values()));
      } else {
        resources = ImmutableList.of();
      }

      if (LOG.isLoggable(Level.FINE)) {
        LOG.fine("Setting task resources to "
            + Iterables.transform(resources, Protobufs.SHORT_TOSTRING));
      }
      TaskInfo.Builder taskBuilder =
          TaskInfo.newBuilder().setName(Tasks.jobKey(task))
              .setTaskId(TaskID.newBuilder().setValue(task.getTaskId()))
              .setSlaveId(slaveId)
              .addAllResources(resources)
              .setData(ByteString.copyFrom(taskInBytes));


      ExecutorInfo executor = ExecutorInfo.newBuilder()
          .setCommand(CommandUtil.create(executorPath))
          .setExecutorId(getExecutorId(task.getTaskId()))
          .setName(EXECUTOR_NAME)
          .setSource(getSourceName(config))
          .addResources(Resources.makeMesosResource(Resources.CPUS, CPUS))
          .addResources(
              Resources.makeMesosResource(Resources.RAM_MB, RAM.as(Data.MB)))
          .build();
      return taskBuilder
          .setExecutor(executor)
          .build();
    }
  }
}
