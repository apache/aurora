package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.twitter.common.BuildInfo;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.mesos.gen.ExecutorStatus;

import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Tracks the status of executors, and manages restarts when build mismatches are found.
 *
 * @author wfarner
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
    } else if (!buildProperties.containsKey(BuildInfo.Key.TIMESTAMP.value)) {
      LOG.severe(BuildInfo.Key.TIMESTAMP.value + " missing from build properties,"
                 + "unable to compare executor revisions for auto build sync!");
      enableSync = false;
    } else {
      enableSync = true;
    }

    this.restartQueue = restartQueue;
  }

  @Override
  public void start(final Closure<String> restartCallback) {
    if (!enableSync) {
      LOG.warning("Status tracking not possible without sufficient build information.");
      return;
    }

    Runnable updater = new Runnable() {
      @Override public void run() {
        if (restartQueue.isEmpty()) return;


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
    String localBuildTime = buildProperties.getProperty(BuildInfo.Key.TIMESTAMP.value);
    String executorBuild = status.getBuildGitRevision();
    String executorBuildTime = status.getBuildTimestamp();

    boolean restart = false;
    if (!localBuild.equals(executorBuild)) {
      LOG.info(String.format("Executor %s has build %s, local build is %s...scheduling restart.",
          status.getSlaveId(), executorBuild, localBuild));
      restart = true;
    } else if (!localBuildTime.equals(executorBuildTime)) {
      LOG.info(String.format("Executor %s built at %s, local build %s...scheduling restart.",
          status.getSlaveId(), executorBuildTime, localBuildTime));
      restart = true;
    }

    if (restart && !restartQueue.contains(status.getSlaveId())) {
      restartQueue.add(status.getSlaveId());
    }
  }
}
