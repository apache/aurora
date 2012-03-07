package com.twitter.mesos.scheduler;

import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskDescription;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.AssignedTask;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A receiver of resource offers and task status updates.
 *
 * @author William Farner
 */
public interface TaskLauncher {

  /**
   * Grants a resource offer to the task launcher, which will be passed to any subsequent task
   * launchers if this one does not accept.
   *
   * @param offer The resource offer.
   * @return A task description, absent if the launcher chooses not to accept the offer.
   */
  Optional<TaskDescription> createTask(Offer offer);

  /**
   * Informs the launcher that a status update has been received for a task.  If the task is not
   * associated with the launcher, it should return {@code false} so that another launcher may
   * receive it.
   *
   * @param status The status update.
   * @return {@code true} if the status is relevant to the launcher and should not be delivered to
   * other launchers, {@code false} otherwise.
   */
  boolean statusUpdate(TaskStatus status);

  /**
   * Task launcher utility class.
   */
  final class Util {
    private static final Logger LOG = Logger.getLogger(Util.class.getName());

    private static final String THERMOS_EXECUTOR_ID_PREFIX = "thermos-";

    // TODO(wickman):  This belongs in SchedulerModule eventually.
    @NotNull
    @CmdLine(name = "thermos_executor_path", help = "Path to the thermos executor launch script.")
    private static final Arg<String> THERMOS_EXECUTOR_PATH = Arg.create();

    private Util() {
      // Utility class.
    }

    /**
     * Creates a mesos task description.
     *
     * @param task Assigned task to translate into a task description.
     * @param slaveId Id of the slave the task is being assigned to.
     * @param selectedPorts Selected ports to include in task resources.
     * @return A new task description.
     * @throws SchedulerException If the task could not be encoded.
     */
    public static TaskDescription makeMesosTask(AssignedTask task, SlaveID slaveId,
        Set<Integer> selectedPorts) throws SchedulerException {

      checkNotNull(task);
      byte[] taskInBytes;
      try {
        taskInBytes = ThriftBinaryCodec.encode(task);
      } catch (ThriftBinaryCodec.CodingException e) {
        LOG.log(Level.SEVERE, "Unable to serialize task.", e);
        throw new SchedulerException("Internal error.", e);
      }

      List<Resource> resources = Resources.from(task.getTask()).toResourceList(selectedPorts);

      LOG.info("Setting task resources to " + resources);
      TaskDescription.Builder assignedTaskBuilder =
          TaskDescription.newBuilder().setName(Tasks.jobKey(task))
              .setTaskId(TaskID.newBuilder().setValue(task.getTaskId()))
              .setSlaveId(slaveId)
              .addAllResources(resources)
              .setData(ByteString.copyFrom(taskInBytes));
      if (Tasks.IS_THERMOS_TASK.apply(task.getTask())) {
        assignedTaskBuilder.setExecutor(ExecutorInfo.newBuilder()
            .setExecutorId(
                ExecutorID.newBuilder().setValue(THERMOS_EXECUTOR_ID_PREFIX + task.getTaskId()))
            .setUri(THERMOS_EXECUTOR_PATH.get()));
      }
      return assignedTaskBuilder.build();
    }
  }
}
