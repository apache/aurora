package com.twitter.mesos.executor;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.stats.SlidingStats;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.Clock;
import com.twitter.mesos.executor.Task.AuditedStatus;
import com.twitter.mesos.executor.util.Disk;
import com.twitter.mesos.gen.ScheduleStatus;

/**
 * A task that is responsible for scanning disk usage of live tasks.
 */
interface DiskUsageScanner extends Runnable {

  static class DiskUsageScannerImpl implements DiskUsageScanner {

    private static final Logger LOG = Logger.getLogger(DiskUsageScannerImpl.class.getName());

    private final AtomicLong scanning = Stats.exportLong("disk_usage_scanner_scanning");
    private final SlidingStats scanTime = new SlidingStats("disk_usage_scan", "ms");
    private final AtomicLong tasksKilled = Stats.exportLong("disk_usage_scanner_tasks_killed");
    private final AtomicLong errors = Stats.exportLong("disk_usage_scanner_errors");

    private final TaskManager taskManager;
    private final Clock clock;

    @Inject
    DiskUsageScannerImpl(TaskManager taskManager, Clock clock) {
      this.taskManager = Preconditions.checkNotNull(taskManager);
      this.clock = Preconditions.checkNotNull(clock);
    }

    @Override
    public void run() {
      scanning.set(1);
      long start = clock.nowMillis();
      for (Task task : taskManager.getLiveTasks()) {
        try {
          Amount<Long, Data> diskUsed =
              Amount.of(Disk.recursiveFileSize(task.getSandboxDir()), Data.BYTES);
          Amount<Long, Data> diskRequested =
              Amount.of(task.getAssignedTask().getTask().getDiskMb(), Data.MB);
          if (diskUsed.compareTo(diskRequested) > 0) {
            LOG.info("Killing task " + task.getId() + " for using " + diskUsed.as(Data.MB)
                + " MB of disk when " + diskRequested.as(Data.MB) + " MB was requested");
            task.terminate(
                new AuditedStatus(ScheduleStatus.FAILED, auditMessage(diskRequested, diskUsed)));
            tasksKilled.incrementAndGet();
          }
        } catch (IOException e) {
          LOG.log(Level.WARNING,
              "Failed to calculate disk usage for " + task.getId() + ": " + e, e);
          errors.incrementAndGet();
        }
      }
      scanTime.accumulate(clock.nowMillis() - start);
      scanning.set(0);
    }

    @VisibleForTesting
    static String auditMessage(Amount<Long, Data> diskRequested, Amount<Long, Data> diskUsed) {
      return "Disk use (" + diskUsed.as(Data.MB) + " MB) "
          + " exceeds request (" + diskRequested.as(Data.MB) + " MB)";
    }
  }
}
