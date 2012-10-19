package com.twitter.mesos.scheduler.periodic;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.BindingAnnotation;
import com.google.inject.Inject;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import com.twitter.common.stats.Stats;
import com.twitter.mesos.Protobufs;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Constraint;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.MesosTaskFactory;
import com.twitter.mesos.scheduler.PulseMonitor;
import com.twitter.mesos.scheduler.TaskLauncher;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task launcher that periodically accepts offers to run bootstrap tasks on hosts and ensure that
 * an executor is available.
 */
public class BootstrapTaskLauncher implements TaskLauncher {

  @VisibleForTesting
  static final String TASK_ID_PREFIX = "system-bootstrap-";

  /**
   * Binding annotation for bootstrap-related fields.
   */
  @BindingAnnotation
  @Target({ FIELD, PARAMETER, METHOD }) @Retention(RUNTIME)
  public @interface Bootstrap { }

  private static final Logger LOG = Logger.getLogger(BootstrapTaskLauncher.class.getName());

  private final AtomicLong executorBootstraps = Stats.exportLong("executor_bootstraps");

  private final PulseMonitor<String> pulseMonitor;
  private final MesosTaskFactory taskFactory;

  @Inject
  BootstrapTaskLauncher(@Bootstrap PulseMonitor<String> pulseMonitor,
      MesosTaskFactory taskFactory) {

    this.pulseMonitor = checkNotNull(pulseMonitor);
    this.taskFactory = checkNotNull(taskFactory);
  }

  @Override
  public Optional<TaskInfo> createTask(Offer offer) {
    String hostname = offer.getHostname();
    if (pulseMonitor.isAlive(offer.getHostname())) {
      return Optional.absent();
    }

    LOG.info("Pulse monitor considers executor dead, attempting to launch bootstrap task on: "
        + hostname);

    pulseMonitor.pulse(offer.getHostname());
    executorBootstraps.incrementAndGet();
    return Optional.of(launchTask(offer));
  }

  @Override
  public boolean statusUpdate(TaskStatus status) {
    if (status.getTaskId().getValue().startsWith(TASK_ID_PREFIX)) {
      LOG.fine("Received status update for bootstrap task: " + Protobufs.toString(status));
      return true;
    } else {
      return false;
    }
  }

  private TaskInfo launchTask(Offer offer) {
    TwitterTaskInfo task = new TwitterTaskInfo()
        .setOwner(new Identity("mesos", "mesos"))
        .setJobName("executor_bootstrap")
        .setNumCpus(0.25)
        .setRamMb(1)
        .setDiskMb(1)
        .setShardId(0)
        .setRequestedPorts(ImmutableSet.<String>of())
        .setConstraints(ImmutableSet.<Constraint>of())
        .setStartCommand("echo \"Bootstrapping\"");
    AssignedTask assignedTask = new AssignedTask(
        TASK_ID_PREFIX + UUID.randomUUID().toString(),
        offer.getHostname(),
        offer.getSlaveId().getValue(),
        task,
        ImmutableMap.<String, Integer>of());
    return taskFactory.createFrom(assignedTask, offer.getSlaveId());
  }

  @Override
  public void cancelOffer(OfferID offer) {
    // No-op.
  }
}
