package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableSet;

import org.apache.commons.io.FileUtils;

import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Garbage collector to reclaim disk space consumed by unused files.
 *
 * @author William Farner
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

      long diskUsedBytes = fileSize(scanDirectory);
      long bytesToReclaim = diskUsedBytes - gcThreshold.as(Data.BYTES);
      if (bytesToReclaim > 0) {
        LOG.info("Triggering " + name + " GC, need to reclaim: "
                 + Amount.of(bytesToReclaim, Data.BYTES).as(Data.MB) + " MB.");
        List<File> files = Arrays.asList(safeListFiles(scanDirectory, fileFilter));

        LOG.info(name + " found " + files.size() + " GC candidates.");

        ImmutableSet.Builder<File> deletedBuilder = ImmutableSet.builder();

        long bytesReclaimed = 0;
        for (File file : files) {
          if (bytesReclaimed >= bytesToReclaim) {
            break;
          }

          long fileSize = fileSize(file);

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

  private static long fileSize(File f) throws IOException {
    return f.isFile() ? f.length() : sizeOfDirectory(f);
  }

  // TODO(wfarner): Figure out a better long-term plan for the code below.  Right now, it is
  //     a shameless copy from org.apache.commons.io.FileUtils to quickly plug an issue caused
  //     by symlinks present in a directory.
  public static long sizeOf(File file) throws IOException {
    if (FileUtils.isSymlink(file)) {
      LOG.warning("Skipping symlink " + file);
      return 0;
    }

    if (!file.exists()) {
      String message = file + " does not exist";
      throw new FileNotFoundException(message);
    }

    if (file.isDirectory()) {
      return sizeOfDirectory(file);
    } else {
      return file.length();
    }
  }

  public static long sizeOfDirectory(File directory) throws IOException {
    if (FileUtils.isSymlink(directory)) {
      LOG.warning("Skipping symlink " + directory);
      return 0;
    }

    if (!directory.exists()) {
      String message = directory + " does not exist";
      throw new FileNotFoundException(message);
    }

    if (!directory.isDirectory()) {
      String message = directory + " is not a directory";
      throw new IllegalArgumentException(message);
    }

    long size = 0;

    File[] files = directory.listFiles();
    if (files == null) {  // null if security restricted
      return 0L;
    }
    for (File file : files) {
      try {
        size += sizeOf(file);
      } catch (IOException e) {
        LOG.warning("Failed to calculate size of " + file +  ", " + e.getMessage());
      }
    }

    return size;
  }
}
