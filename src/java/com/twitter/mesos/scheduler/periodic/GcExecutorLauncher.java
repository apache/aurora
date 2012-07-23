package com.twitter.mesos.scheduler.periodic;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;
import com.google.protobuf.ByteString;

import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.comm.AdjustRetainedTasks;
import com.twitter.mesos.scheduler.CommandUtil;
import com.twitter.mesos.scheduler.PulseMonitor;
import com.twitter.mesos.scheduler.Resources;
import com.twitter.mesos.scheduler.StateManager;
import com.twitter.mesos.scheduler.TaskLauncher;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

import static com.twitter.mesos.scheduler.periodic.HistoryPruneRunner.IS_THERMOS;
import static com.twitter.mesos.scheduler.periodic.HistoryPruneRunner.TASK_TO_HOST;
import static com.twitter.mesos.scheduler.periodic.HistoryPruneRunner.retainedTasksMessage;

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
      new Resources(0.2, Amount.of(64L, Data.MB), Amount.of(16L, Data.MB), 0);

  private static final String SYSTEM_TASK_PREFIX = "system-gc-";
  private static final String EXECUTOR_PREFIX = "gc-";

  private final PulseMonitor<String> pulseMonitor;
  private final Optional<String> gcExecutorPath;
  private final StateManager stateManager;
  private final HistoryPruner historyPruner;

  @Inject
  GcExecutorLauncher(
      @GcExecutor PulseMonitor<String> pulseMonitor,
      @GcExecutor Optional<String> gcExecutorPath,
      StateManager stateManager,
      HistoryPruner historyPruner) {

    this.gcExecutorPath = checkNotNull(gcExecutorPath);
    this.pulseMonitor = checkNotNull(pulseMonitor);
    this.stateManager = checkNotNull(stateManager);
    this.historyPruner = checkNotNull(historyPruner);
  }

  @Override
  public Optional<TaskInfo> createTask(Offer offer) {
    if (!gcExecutorPath.isPresent() || pulseMonitor.isAlive(offer.getHostname())) {
      return Optional.absent();
    }

    Set<ScheduledTask> allInactiveTasks =
        stateManager.fetchTasks(HistoryPruneRunner.INACTIVE_QUERY);
    Set<ScheduledTask> inactiveThermosTasks =
        ImmutableSet.copyOf(Iterables.filter(allInactiveTasks, IS_THERMOS));
    Set<ScheduledTask> prunedTasks =
        HistoryPruneRunner.getPrunedTasks(inactiveThermosTasks, historyPruner);

    String host = offer.getHostname();

    Predicate<ScheduledTask> sameHost =
        Predicates.compose(Predicates.equalTo(host), TASK_TO_HOST);
    Set<ScheduledTask> hostPruned = ImmutableSet.copyOf(Iterables.filter(prunedTasks, sameHost));
    Map<String, ScheduledTask> hostPrunedById = Tasks.mapById(hostPruned);
    Set<ScheduledTask> onHost = ImmutableSet.copyOf(Iterables.filter(
        stateManager.fetchTasks(HistoryPruneRunner.hostQuery(host)), IS_THERMOS));

    AdjustRetainedTasks message = retainedTasksMessage(onHost, hostPruned);
    byte[] data;
    try {
      data = ThriftBinaryCodec.encode(message);
    } catch (CodingException e) {
      LOG.severe("Failed to encode retained tasks message: " + message);
      return Optional.absent();
    }

    pulseMonitor.pulse(offer.getHostname());

    if (!hostPrunedById.isEmpty()) {
      stateManager.deleteTasks(hostPrunedById.keySet());
    }

    String uuid = UUID.randomUUID().toString();
    ExecutorInfo.Builder executor = ExecutorInfo.newBuilder()
        .setExecutorId(ExecutorID.newBuilder().setValue(EXECUTOR_PREFIX + uuid))
        .setCommand(CommandUtil.create(gcExecutorPath.get()));

    return Optional.of(TaskInfo.newBuilder().setName("system-gc")
        .setTaskId(TaskID.newBuilder().setValue(SYSTEM_TASK_PREFIX + uuid))
        .setSlaveId(offer.getSlaveId())
        .addAllResources(GC_EXECUTOR_RESOURCES.toResourceList())
        .setData(ByteString.copyFrom(data))
        .setExecutor(executor)
        .build());
  }

  @Override
  public boolean statusUpdate(TaskStatus status) {
    if (status.getTaskId().getValue().startsWith(SYSTEM_TASK_PREFIX)) {
      LOG.info("Received status update for GC task: " + status);
      return true;
    } else {
      return false;
    }
  }
}
