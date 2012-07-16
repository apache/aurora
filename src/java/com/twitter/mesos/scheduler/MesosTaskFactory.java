package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;

import static com.google.common.base.Preconditions.checkNotNull;

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

    // TODO(wfarner): Push these args out to a module once java executor is gone.

    @NotNull
    @CmdLine(name = "executor_path", help = "Path to the executor launch script.")
    private static final Arg<String> EXECUTOR_PATH = Arg.create();

    @NotNull
    @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor launch script.")
    private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

    @CmdLine(name = "executor_env",
        help = "Environment variables to set for the executor launch script.")
    private static final Arg<Map<String, String>> EXECUTOR_ENV =
        Arg.<Map<String, String>>create(ImmutableMap.<String, String>of());

    @Positive
    @CmdLine(name = "executor_resources_cpus",
        help = "The number of CPUS that should be reserved by mesos for the executor.")
    private static final Arg<Double> EXECUTOR_CPUS = Arg.create(0.25);

    @Positive
    @CmdLine(name = "executor_resources_ram",
        help = "The amount of RAM that should be reserved by mesos for the executor.")
    private static final Arg<Amount<Double, Data>> EXECUTOR_RAM =
        Arg.create(Amount.of(2d, Data.GB));

    private final String exepath;
    private final ImmutableMap<String, String> env;
    private final double cpus;
    private final Amount<Double, Data> ram;

    @Inject
    MesosTaskFactoryImpl() {
      this(EXECUTOR_PATH.get(), EXECUTOR_CPUS.get(), EXECUTOR_RAM.get(), EXECUTOR_ENV.get());
    }

    MesosTaskFactoryImpl(String executorPath, double cpus, Amount<Double, Data> ram,
        Map<String, String> env) {

      Preconditions.checkArgument(cpus > 0);

      Preconditions.checkNotNull(ram);
      Preconditions.checkArgument(ram.getValue() > 0);

      this.exepath = MorePreconditions.checkNotBlank(executorPath);
      this.env = ImmutableMap.copyOf(env);
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

      LOG.info("Setting task resources to " + resources);
      TaskInfo.Builder taskBuilder =
          TaskInfo.newBuilder().setName(Tasks.jobKey(task))
              .setTaskId(TaskID.newBuilder().setValue(task.getTaskId()))
              .setSlaveId(slaveId)
              .addAllResources(resources)
              .setData(ByteString.copyFrom(taskInBytes));

      ExecutorInfo executor;
      if (Tasks.IS_THERMOS_TASK.apply(task.getTask())) {
        executor = ExecutorInfo.newBuilder()
            .setCommand(CommandUtil.create(THERMOS_EXECUTOR_PATH.get(), env))
            .setExecutorId(
                ExecutorID.newBuilder().setValue(THERMOS_EXECUTOR_ID_PREFIX + task.getTaskId()))
            .build();
      } else {
        executor = ExecutorInfo.newBuilder().setCommand(CommandUtil.create(exepath, env))
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
