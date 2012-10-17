package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * A factory to create mesos task objects.
 */
public interface MesosTaskFactory {

  ExecutorID DEFAULT_EXECUTOR_ID = ExecutorID.newBuilder().setValue("twitter").build();

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

    private static final String THERMOS_EXECUTOR_ID_PREFIX = "thermos-";

    static class ExecutorConfig {
      private final String oldExecutorPath;
      private final String thermosExecutorPath;

      ExecutorConfig(String oldExecutorPath, String thermosExecutorPath) {
        this.oldExecutorPath = checkNotBlank(oldExecutorPath);
        this.thermosExecutorPath = checkNotBlank(thermosExecutorPath);
      }

      String getOldExecutorPath() {
        return oldExecutorPath;
      }

      String getThermosExecutorPath() {
        return thermosExecutorPath;
      }
    }

    // These are hard-coded to avoid risk posed due to MESOS-911.
    private static final double EXECUTOR_CPUS = 0.25;
    private static final Amount<Double, Data> EXECUTOR_RAM = Amount.of(3d, Data.GB);

    private final String oldExePath;
    private final String thermosExePath;
    private final double cpus;
    private final Amount<Double, Data> ram;

    @Inject
    MesosTaskFactoryImpl(ExecutorConfig executorConfig) {
      this(executorConfig, EXECUTOR_CPUS, EXECUTOR_RAM);
    }

    @VisibleForTesting
    MesosTaskFactoryImpl(ExecutorConfig executorConfig, double cpus, Amount<Double, Data> ram) {
      Preconditions.checkNotNull(executorConfig);
      Preconditions.checkArgument(cpus > 0);

      Preconditions.checkNotNull(ram);
      Preconditions.checkArgument(ram.getValue() > 0);

      this.oldExePath = executorConfig.getOldExecutorPath();
      this.thermosExePath = executorConfig.getThermosExecutorPath();
      this.cpus = cpus;
      this.ram = ram;
    }

    @Override public TaskInfo createFrom(AssignedTask task, SlaveID slaveId)
        throws SchedulerException {

      checkNotNull(task);
      byte[] taskInBytes;
      try {
        taskInBytes = ThriftBinaryCodec.encode(task);
      } catch (ThriftBinaryCodec.CodingException e) {
        LOG.log(Level.SEVERE, "Unable to serialize task.", e);
        throw new SchedulerException("Internal error.", e);
      }

      List<Resource> resources;
      if (task.isSetAssignedPorts()) {
        resources = Resources.from(task.getTask())
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

      ExecutorInfo executor;
      if (Tasks.IS_THERMOS_TASK.apply(task.getTask())) {
        executor = ExecutorInfo.newBuilder()
            .setCommand(CommandUtil.create(thermosExePath))
            .setExecutorId(
                ExecutorID.newBuilder().setValue(THERMOS_EXECUTOR_ID_PREFIX + task.getTaskId()))
            .addResources(Resources.makeMesosResource(Resources.CPUS, ThermosResources.CPUS))
            .addResources(
                Resources.makeMesosResource(Resources.RAM_MB, ThermosResources.RAM.as(Data.MB)))
            .build();
      } else {
        executor = ExecutorInfo.newBuilder().setCommand(CommandUtil.create(oldExePath))
            .setExecutorId(DEFAULT_EXECUTOR_ID)
            .addResources(Resources.makeMesosResource(Resources.CPUS, cpus))
            .addResources(Resources.makeMesosResource(Resources.RAM_MB, ram.as(Data.MB)))
            .build();
      }

      return taskBuilder
          .setExecutor(executor)
          .build();
    }
  }
}
