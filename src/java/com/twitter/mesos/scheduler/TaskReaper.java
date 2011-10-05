package com.twitter.mesos.scheduler;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;

import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.mesos.ExecutorKey;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Scans for and reaps tasks that are associated with executors that are considered to be
 * decommissioned.
 *
 * @author William Farner
 */
class TaskReaper {

  private static final Logger LOG = Logger.getLogger(TaskReaper.class.getName());
  private static final Amount<Long, Time> SHUTDOWN_GRACE_PERIOD = Amount.of(1L, Time.SECONDS);

  private final StateManager stateManager;
  private final Supplier<Set<ExecutorKey>> knownExecutorSupplier;

  @Inject
  TaskReaper(StateManager stateManager, Supplier<Set<ExecutorKey>> knownExecutorSupplier) {
    this.stateManager = checkNotNull(stateManager);
    this.knownExecutorSupplier = checkNotNull(knownExecutorSupplier);
  }

  /**
   * Starts the task reaper.
   *
   * @param startDelay Time to wait before running the first scan.
   * @param scanInterval Interval between subsequent scans.
   * @return A command that can be invoked to halt the reaper.
   */
  Command start(Amount<Long, Time> startDelay, Amount<Long, Time> scanInterval) {
    final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
        new ThreadFactoryBuilder()
            .setNameFormat("TaskReaper-%d")
            .setDaemon(true)
            .build());

    Runnable poller = new Runnable() {
      @Override public void run() {
        try {
          stateManager.scanOutstandingTasks();
          Set<String> abandonedTasks = getAbandonedTasks(stateManager);
          if (!abandonedTasks.isEmpty()) {
            stateManager.abandonTasks(abandonedTasks);
          }
        } catch (IllegalStateException e) {
          LOG.log(Level.WARNING, "Task reaper could not modify state manager.", e);
        }
      }
    };

    executor.scheduleAtFixedRate(poller,
        startDelay.as(Time.MILLISECONDS),
        scanInterval.as(Time.MILLISECONDS),
        TimeUnit.MILLISECONDS);

    return new Command() {
      @Override public void execute() {
        new ExecutorServiceShutdown(executor, SHUTDOWN_GRACE_PERIOD);
      }
    };
  }

  private static final Function<ExecutorKey, String> KEY_TO_HOST =
      new Function<ExecutorKey, String>() {
        @Override public String apply(ExecutorKey key) {
          return key.hostname;
        }
      };

  @VisibleForTesting
  Set<String> getAbandonedTasks(StateManager stateManager) {
    Set<String> knownExecutorHosts =
        ImmutableSet.copyOf(Iterables.transform(knownExecutorSupplier.get(), KEY_TO_HOST));

    Multimap<String, String> hostToTasks = stateManager.getHostAssignedTasks();
    Set<String> deadExecutors = Sets.difference(hostToTasks.keySet(), knownExecutorHosts);

    if (!deadExecutors.isEmpty()) {
      LOG.info("Found dead executors: " + deadExecutors);
    }

    ImmutableSet.Builder<String> abandonedTasks = ImmutableSet.builder();
    for (String deadExecutor : deadExecutors) {
      abandonedTasks.addAll(hostToTasks.get(deadExecutor));
    }
    return abandonedTasks.build();
  }
}
