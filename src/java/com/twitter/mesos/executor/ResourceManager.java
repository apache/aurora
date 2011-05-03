package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileFilter;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.common.application.ActionRegistry;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;

/**
 * Manages system resources and periodically gathers information about resource consumption by
 * tasks.
 *
 * @author William Farner
 */
public class ResourceManager {
  private static final Logger LOG = Logger.getLogger(ResourceManager.class.getName());

  // TODO(William Farner): These need to be configurable.
  private static final Amount<Long, Time> FILE_EXPIRATION_TIME = Amount.of(1L, Time.DAYS);
  private static final Amount<Long, Data> MAX_DISK_SPACE = Amount.of(20L, Data.GB);

  private final File managedDir;
  private final TaskManager taskManager;
  private final ActionRegistry shutdownRegistry;
  // TODO(William Farner): Enable this if we decide to use this instead of pushing it into the mesos
  //    core.
  //private final ResourceScanner resourceScanner;

  public ResourceManager(TaskManager taskManager, File managedDir,
      ActionRegistry shutdownRegistry) {
    this.managedDir = Preconditions.checkNotNull(managedDir);
    Preconditions.checkArgument(managedDir.exists(),
        "Managed directory does not exist: " + managedDir);

    this.taskManager = Preconditions.checkNotNull(taskManager);
    //this.resourceScanner = new LinuxProcScraper();

    this.shutdownRegistry = Preconditions.checkNotNull(shutdownRegistry);
  }

  public void start() {
    //startResourceScanner();
    startDiskGc();
  }

  /*
  private void startResourceScanner() {
    Runnable scanner = new Runnable() {
      @Override public void run() {
        for (Task task : taskManager.getTaskRunners()) {
          // TODO(William Farner): Need to track rate of jiffies to determine CPU usage.
          // TODO(William Farner): Remove this hack once the mesos slave does its own resource tracking.
          if (task instanceof TaskRunner) {
            TaskRunner taskRunner = (TaskRunner) task;
            ResourceScanner.ProcInfo procInfo =
                resourceScanner.getResourceUsage(taskRunner.getProcessId(), task.getRootDir());
            task.getResourceConsumption()
                .setDiskUsedMb(procInfo.getDiskUsed().as(Data.MB).intValue())
                .setMemUsedMb(procInfo.getVmSize().as(Data.MB).intValue());

            LOG.info("Resource usage for task " + task.getId() + ": " + procInfo);

          }
        }
      }
    };

    ScheduledExecutorService scannerExecutor = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Proc-Scraper-%d").build());
    scannerExecutor.scheduleAtFixedRate(scanner, 10, 10, TimeUnit.SECONDS);
  }
  */

  private void startDiskGc() {
    FileFilter expiredOrUnknown = new FileFilter() {
        @Override public boolean accept(File file) {
          if (!file.isDirectory()) {
            return false;
          }

          if (!taskManager.hasTask(file.getName())) {
            // Always delete unknown directories.
            return true;
          } else if (taskManager.isRunning(file.getName())) {
            // Don't delete files for running tasks.
            return false;
          }

          // If the directory is for a known task, only delete when it has expired.
          long timeSinceLastModify =
              System.currentTimeMillis() - DiskGarbageCollector.recursiveLastModified(file);
          return timeSinceLastModify > FILE_EXPIRATION_TIME.as(Time.MILLISECONDS);
        }
      };

    Closure<File> gcCallback = new Closure<File>() {
      @Override public void execute(File file) {
        LOG.info("Removing record for garbage-collected task "  + file.getName());
        taskManager.deleteCompletedTask(file.getName());
      }
    };

    // The expired file GC always runs, and expunges all directories that are unknown or too old.
    DiskGarbageCollector expiredDirGc = new DiskGarbageCollector("ExpiredOrUnknownDir",
        managedDir, expiredOrUnknown, gcCallback);

    FileFilter completedTaskFileFilter = new FileFilter() {
        @Override public boolean accept(File file) {
          if (!file.isDirectory()) return false;
          return !taskManager.isRunning(file.getName());
        }
      };

    // The completed task GC only runs when disk is exhausted, and removes directories for tasks
    // that have completed.
    DiskGarbageCollector completedTaskGc = new DiskGarbageCollector("CompletedTask",
        managedDir, completedTaskFileFilter, MAX_DISK_SPACE, gcCallback);

    // TODO(William Farner): Make GC intervals configurable.
    final ScheduledExecutorService gcExecutor = new ScheduledThreadPoolExecutor(2,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Disk GC-%d").build());
    gcExecutor.scheduleAtFixedRate(expiredDirGc, 1, 5, TimeUnit.MINUTES);
    gcExecutor.scheduleAtFixedRate(completedTaskGc, 2, 1, TimeUnit.MINUTES);
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() throws RuntimeException {
        LOG.info("Shutting down gc executor.");
        gcExecutor.shutdownNow();
      }
    });
  }
}
