package com.twitter.mesos.scheduler;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.BuildInfo;
import com.twitter.mesos.gen.comm.ExecutorStatus;

/**
 * Tracks the status of executors, and manages restarts when build mismatches are found.
 *
 * @author William Farner
 */
public class ExecutorTrackerImpl implements ExecutorTracker {

  private static final Logger LOG = Logger.getLogger(ExecutorTrackerImpl.class.getName());

  private static final Amount<Long, Time> EXECUTOR_RESTART_INTERVAL = Amount.of(30L, Time.SECONDS);

  private final Queue<String> restartQueue;

  private final Properties buildProperties;
  private final boolean enableSync;

  @Inject
  public ExecutorTrackerImpl(BuildInfo buildInfo) {
    this(buildInfo, new ConcurrentLinkedQueue<String>());
  }

  public ExecutorTrackerImpl(BuildInfo buildInfo, Queue<String> restartQueue) {
    this.buildProperties = Preconditions.checkNotNull(buildInfo).getProperties();

    if (!buildProperties.containsKey(BuildInfo.Key.GIT_REVISION.value)) {
      LOG.severe(BuildInfo.Key.GIT_REVISION.value + " missing from build properties,"
                 + "unable to compare executor revisions for auto build sync!");
      enableSync = false;
    } else {
      enableSync = true;
    }

    this.restartQueue = restartQueue;

    Stats.exportSize("executor_restarts_pending", restartQueue);
  }

  @Override
  public void start(final Closure<String> restartCallback) {
    if (!enableSync) {
      LOG.warning("Status tracking not possible without sufficient build information.");
      return;
    }

    Runnable updater = new Runnable() {
      @Override public void run() {
        if (restartQueue.isEmpty()) {
          return;
        }

        String slaveId = restartQueue.remove();
        LOG.info("Requesting that slave be restarted: " + slaveId);
        restartCallback.execute(slaveId);
      }
    };

    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("ExecutorTracker-%d").build();

    new ScheduledThreadPoolExecutor(1, threadFactory).scheduleAtFixedRate(
        updater, EXECUTOR_RESTART_INTERVAL.as(Time.SECONDS),
        EXECUTOR_RESTART_INTERVAL.as(Time.SECONDS), TimeUnit.SECONDS
    );
  }

  @Override
  public void addStatus(ExecutorStatus status) {
    Preconditions.checkNotNull(status);
    Preconditions.checkNotNull(status.getSlaveId());

    String localBuild = buildProperties.getProperty(BuildInfo.Key.GIT_REVISION.value);
    String executorBuild = status.getBuildGitRevision();

    boolean restart = false;
    if (!localBuild.equals(executorBuild)) {
      LOG.info(String.format("Executor %s has build %s, local build is %s...scheduling restart.",
          status.getHost(), executorBuild, localBuild));
      restart = true;
    }

    if (restart && !restartQueue.contains(status.getSlaveId())) {
      vars.executorRestartsRequested.incrementAndGet();
      restartQueue.add(status.getSlaveId());
    }
  }

  private class Vars {
    final AtomicLong executorRestartsRequested = Stats.exportLong("executor_restarts_requested");
  }
  private Vars vars = new Vars();
}
