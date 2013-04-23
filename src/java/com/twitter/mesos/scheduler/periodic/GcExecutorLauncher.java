package com.twitter.mesos.scheduler.periodic;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.Protobufs;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.scheduler.CommandUtil;
import com.twitter.mesos.scheduler.PulseMonitor;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.TaskLauncher;
import com.twitter.mesos.scheduler.configuration.Resources;
import com.twitter.mesos.scheduler.storage.Storage;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task launcher that periodically initiates a garbage-collecting executor on a host.
 */
public class GcExecutorLauncher implements TaskLauncher {
  private static final Logger LOG = Logger.getLogger(GcExecutorLauncher.class.getName());

  /**
   * Binding annotation for gc executor-related fields..
   */
  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface GcExecutor { }

  private static final Resources GC_EXECUTOR_RESOURCES =
      new Resources(0.19, Amount.of(127L, Data.MB), Amount.of(15L, Data.MB), 0);
  private static final Resources ALMOST_EMPTY_TASK_RESOURCES =
      new Resources(0.01, Amount.of(1L, Data.MB), Amount.of(1L, Data.MB), 0);

  private static final String SYSTEM_TASK_PREFIX = "system-gc-";
  private static final String EXECUTOR_NAME = "aurora.gc";
  private static final String EXECUTOR_PREFIX = "gc-";

  private final PulseMonitor<String> pulseMonitor;
  private final Optional<String> gcExecutorPath;
  private final Storage storage;

  @Inject
  GcExecutorLauncher(
      @GcExecutor PulseMonitor<String> pulseMonitor,
      @GcExecutor Optional<String> gcExecutorPath,
      Storage storage) {

    this.pulseMonitor = checkNotNull(pulseMonitor);
    this.gcExecutorPath = checkNotNull(gcExecutorPath);
    this.storage = checkNotNull(storage);
  }

  static String getSourceName(String hostname, String uuid) {
    return String.format("%s.%s.%s", EXECUTOR_NAME, hostname, uuid);
  }

  @Override
  public Optional<TaskInfo> createTask(Offer offer) {
    if (!gcExecutorPath.isPresent() || pulseMonitor.isAlive(offer.getHostname())) {
      return Optional.absent();
    }

    Set<ScheduledTask> tasksOnHost =
        Storage.Util.fetchTasks(storage, Query.bySlave(offer.getHostname()));
    AdjustRetainedTasks message = new AdjustRetainedTasks()
        .setRetainedTasks(Maps.transformValues(Tasks.mapById(tasksOnHost), Tasks.GET_STATUS));
    byte[] data;
    try {
      data = ThriftBinaryCodec.encode(message);
    } catch (CodingException e) {
      LOG.severe("Failed to encode retained tasks message: " + message);
      return Optional.absent();
    }

    pulseMonitor.pulse(offer.getHostname());

    String uuid = UUID.randomUUID().toString();
    ExecutorInfo.Builder executor = ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue(EXECUTOR_PREFIX + uuid))
        .setName(EXECUTOR_NAME)
        .setSource(getSourceName(offer.getHostname(), uuid))
        .addAllResources(GC_EXECUTOR_RESOURCES.toResourceList())
        .setCommand(CommandUtil.create(gcExecutorPath.get()));

    return Optional.of(TaskInfo.newBuilder().setName("system-gc")
        .setTaskId(TaskID.newBuilder().setValue(SYSTEM_TASK_PREFIX + uuid))
        .setSlaveId(offer.getSlaveId())
        .setData(ByteString.copyFrom(data))
        .setExecutor(executor)
        .addAllResources(ALMOST_EMPTY_TASK_RESOURCES.toResourceList())
        .build());
  }

  @Override
  public boolean statusUpdate(TaskStatus status) {
    if (status.getTaskId().getValue().startsWith(SYSTEM_TASK_PREFIX)) {
      LOG.info("Received status update for GC task: " + Protobufs.toString(status));
      return true;
    } else {
      return false;
    }
  }

  @Override
  public void cancelOffer(OfferID offer) {
    // No-op.
  }
}
