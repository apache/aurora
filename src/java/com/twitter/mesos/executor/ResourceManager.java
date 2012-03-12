package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages system resources and periodically gathers information about resource consumption by
 * tasks.
 *
 * @author William Farner
 */
public class ResourceManager {
  private static final Logger LOG = Logger.getLogger(ResourceManager.class.getName());

  // TODO(William Farner): Wire this through a module.
  // Slaves are configured with 400k MB of disk, which is approx 390 GB.
  @CmdLine(name = "max_disk_space",
           help = "Maximum amount of consumed sandbox disk before proactively reclaiming.")
  private static final Arg<Amount<Long, Data>> MAX_DISK_SPACE =
      Arg.create(Amount.of(390L, Data.GB));

  private final File managedDir;
  private final TaskManager taskManager;
  private final ShutdownRegistry shutdownRegistry;
  private final FileDeleter fileDeleter;

  public ResourceManager(TaskManager taskManager,
      File managedDir,
      ShutdownRegistry shutdownRegistry,
      FileDeleter fileDeleter) {
    this.managedDir = checkNotNull(managedDir);
    Preconditions.checkArgument(
        managedDir.exists(), "Managed directory does not exist: " + managedDir);

    this.taskManager = checkNotNull(taskManager);
    this.shutdownRegistry = checkNotNull(shutdownRegistry);
    this.fileDeleter = checkNotNull(fileDeleter);
  }

  public void start() {
    startDiskGc();
  }

  private void startDiskGc() {
    FileFilter unknownDir = new FileFilter() {
        @Override public boolean accept(File file) {
          return file.isDirectory() && !taskManager.hasTask(file.getName());
        }
      };

    Closure<Set<File>> deleteFiles = new Closure<Set<File>>() {
      @Override public void execute(Set<File> files) {
        for (File file : files) {
          try {
            fileDeleter.execute(file);
          } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to delete " + file, e);
          }
        }
      }
    };

    Closure<Set<File>> unknownGcCallback = new Closure<Set<File>>() {
      @Override public void execute(Set<File> files) {
        LOG.info("Deleted files associated with unknown tasks: " + files);
      }
    };

    // The expired file GC always runs, and expunges all directories that are unknown or too old.
    DiskGarbageCollector unknownDirGc = new DiskGarbageCollector(
        "UnknownDir",
        managedDir,
        unknownDir,
        Closures.combine(ImmutableList.of(unknownGcCallback, deleteFiles)));

    FileFilter completedTaskFileFilter = new FileFilter() {
        @Override public boolean accept(File file) {
          return file.isDirectory() && !taskManager.isRunning(file.getName());
        }
      };

    Closure<Set<File>> gcCallback = new Closure<Set<File>>() {
      @Override public void execute(Set<File> files) {
        LOG.info("Removing record for garbage-collected tasks "  + files);

        taskManager.deleteCompletedTasks(ImmutableSet.copyOf(Iterables.transform(files,
            new Function<File, String>() {
              @Override public String apply(File file) {
                return file.getName();
              }
            })));
      }
    };

    // The completed task GC only runs when disk is exhausted, and removes directories for tasks
    // that have completed.
    DiskGarbageCollector completedTaskGc = new DiskGarbageCollector(
        "CompletedTask",
        managedDir,
        completedTaskFileFilter,
        MAX_DISK_SPACE.get(),
        Closures.combine(ImmutableList.of(gcCallback, deleteFiles)));

    // TODO(William Farner): Make GC intervals configurable.
    final ScheduledExecutorService gcExecutor = new ScheduledThreadPoolExecutor(2,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Disk GC-%d").build());
    gcExecutor.scheduleAtFixedRate(unknownDirGc, 1, 5, TimeUnit.MINUTES);
    gcExecutor.scheduleAtFixedRate(completedTaskGc, 2, 1, TimeUnit.MINUTES);
    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        LOG.info("Shutting down gc executor.");
        gcExecutor.shutdownNow();
      }
    });
  }
}
