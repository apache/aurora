package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.executor.util.Disk;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Garbage collector to reclaim disk space consumed by unused files.
 */
public class DiskGarbageCollector implements Runnable {
  private static final Logger LOG = Logger.getLogger(DiskGarbageCollector.class.getName());

  private final String name;
  private final File scanDirectory;
  private final FileFilter fileFilter;
  private final Amount<Long, Data> gcThreshold;
  private final Closure<Set<File>> gcCallback;

  /**
   * Creates a new disk garbage collector that will scan the first level of a directory and
   * clean up directories that are permitted by a file filter, ranking the files by last
   * modification time.
   *
   * @param name THe name of the garbage collector.
   * @param scanDirectory Directory to scan (only the first level will be scanned).
   * @param fileFilter Filter to determine which files are candidate for garbage collection.
   * @param gcThreshold Minimum size of the directory before a GC is performed.
   * @param gcCallback Callback to be notified when files are garbage collected.
   */
  public DiskGarbageCollector(String name,
      File scanDirectory,
      FileFilter fileFilter,
      Amount<Long, Data> gcThreshold,
      Closure<Set<File>> gcCallback) {
    this.name = MorePreconditions.checkNotBlank(name);
    this.scanDirectory = checkNotNull(scanDirectory);
    this.fileFilter = checkNotNull(fileFilter);
    this.gcThreshold = checkNotNull(gcThreshold);
    this.gcCallback = gcCallback;
  }

  /**
   * Creates a new disk garbage collector that will run regardless of the amount of disk consumed.
   *
   * @param name THe name of the garbage collector.
   * @param scanDirectory Directory to scan (only the first level will be scanned).
   * @param fileFilter Filter to determine which files are candidate for garbage collection.
   * @param gcCallback Callback to be notified when files are garbage collected.
   */
  public DiskGarbageCollector(String name,
      File scanDirectory,
      FileFilter fileFilter,
      Closure<Set<File>> gcCallback) {
    this(name, scanDirectory, fileFilter, Amount.of(0L, Data.BYTES), gcCallback);
  }

  public static final File[] NO_FILES = new File[0];

  private static File[] safeListFiles(File file, FileFilter filter) {
    if (file.exists() && file.isDirectory()) {
      File[] children = file.listFiles(filter);
      // Guard against a race - if the dir is deleted out from under us we can get null here.
      if (children != null) {
        return children;
      }
    }
    return NO_FILES;
  }

  private static final Function<File, String> FILE_NAME = new Function<File, String>() {
    @Override public String apply(File file) {
      return file.getName();
    }
  };

  @Override
  public void run() {
    try {
      LOG.info("Performing " + name + " garbage collection scan of directory " + scanDirectory);
      if (!scanDirectory.exists()) {
        LOG.info("Directory does not exist, exiting.");
        return;
      }

      if (!scanDirectory.isDirectory()) {
        LOG.severe("Scan directory is not a directory, exiting.");
        return;
      }

      long diskUsedBytes = Disk.recursiveFileSize(scanDirectory);
      long bytesToReclaim = diskUsedBytes - gcThreshold.as(Data.BYTES);
      if (bytesToReclaim > 0) {
        LOG.info("Triggering " + name + " GC, need to reclaim: "
                 + Amount.of(bytesToReclaim, Data.BYTES).as(Data.MB) + " MB.");
        List<File> files = Arrays.asList(safeListFiles(scanDirectory, fileFilter));

        LOG.info(name + " found " + files.size() + " GC candidates.");

        ImmutableSet.Builder<File> deletedBuilder = ImmutableSet.builder();

        long bytesReclaimed = 0;

        // We sort the file names here to ensure that the ordering is guaranteed.  This is baking
        // outside knowledge about how task IDs (and their sandbox directories) are crafted,
        // but since this code is planned for replacement in the very near term, this is considered
        // acceptable.
        for (File file : Ordering.natural().onResultOf(FILE_NAME).sortedCopy(files)) {
          if (bytesReclaimed >= bytesToReclaim) {
            break;
          }

          long fileSize = Disk.recursiveFileSize(file);

          LOG.info(name + " GC reclaiming " + Amount.of(fileSize, Data.BYTES).as(Data.MB)
                   + " MB from " + file);
          bytesReclaimed += fileSize;
          deletedBuilder.add(file);
        }

        Set<File> deletedFiles = deletedBuilder.build();
        if (!deletedFiles.isEmpty()) {
          gcCallback.execute(deletedFiles);
        }

        if (bytesReclaimed < bytesToReclaim) {
          LOG.warning(name + " GC failed to reclaim sufficient disk space!");
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.WARNING, name + " GC encountered an exception.", t);
    }
  }
}
