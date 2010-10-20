package com.twitter.mesos.executor;

import com.google.common.base.Preconditions;
import com.twitter.common.base.Closure;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import org.apache.commons.io.FileUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Garbage collector to reclaim disk space consumed by unused files.
 *
 * @author wfarner
 */
public class DiskGarbageCollector implements Runnable {
  private static final Logger LOG = Logger.getLogger(DiskGarbageCollector.class.getName());

  private final String name;
  private final File scanDirectory;
  private final FileFilter fileFilter;
  private final Amount<Long, Data> gcThreshold;
  private final Closure<File> gcCallback;

  /**
   * Creates a new disk garbage collector that will scan the first level of a directory and
   * clean up directories that are permitted by a file filter, ranking the files by last
   * modification time.
   *
   * @param name THe name of the garbage collector.
   * @param scanDirectory Directory to scan (only the first level will be scanned).
   * @param fileFilter Filter to determine which files are candidate for garbage collection.
   * @param gcThreshold Minimum size of the directory before a GC is performed.
   * @param gcCallback Optional callback to be notified when a file is garbage collected.
   */
  public DiskGarbageCollector(String name, File scanDirectory, FileFilter fileFilter,
      Amount<Long, Data> gcThreshold, @Nullable Closure<File> gcCallback) {
    this.name = MorePreconditions.checkNotBlank(name);
    this.scanDirectory = Preconditions.checkNotNull(scanDirectory);
    this.fileFilter = Preconditions.checkNotNull(fileFilter);
    this.gcThreshold = Preconditions.checkNotNull(gcThreshold);
    this.gcCallback = gcCallback;
  }

  /**
   * Creates a new disk garbage collector that will run regardless of the amount of disk consumed.
   *
   * @param name THe name of the garbage collector.
   * @param scanDirectory Directory to scan (only the first level will be scanned).
   * @param fileFilter Filter to determine which files are candidate for garbage collection.
   * @param gcCallback Optional callback to be notified when a file is garbage collected.
   */
  public DiskGarbageCollector(String name, File scanDirectory, FileFilter fileFilter,
      @Nullable Closure<File> gcCallback) {
    this(name, scanDirectory, fileFilter, Amount.of(0L, Data.BYTES), gcCallback);
  }

  public static long recursiveLastModified(File file) {
    Preconditions.checkNotNull(file);

    if (file.isFile()) {
      return file.lastModified();
    }

    long highestLastModified = file.lastModified();
    for (File f : file.listFiles()) {
      long lastModified = recursiveLastModified(f);
      if (lastModified > highestLastModified) highestLastModified = lastModified;
    }

    return highestLastModified;
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
        List<File> files = Arrays.asList(scanDirectory.listFiles(fileFilter));
        Collections.sort(files, LAST_MODIFIED_COMPARATOR);

        LOG.info(name + " found " + files.size() + " GC candidates.");

        long bytesReclaimed = 0;
        for (File file : files) {
          if (bytesReclaimed >= bytesToReclaim) break;

          long fileSize = fileSize(file);

          LOG.info(name + " GC reclaiming " + Amount.of(fileSize, Data.BYTES).as(Data.MB)
                   + " MB from " + file);

          try {
            if (file.isFile()) {
              file.delete();
            } else {
              FileUtils.deleteDirectory(file);
            }
            bytesReclaimed += fileSize;
            if (gcCallback != null) gcCallback.execute(file);
          } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to GC " + file, e);
          }
        }

        if (bytesReclaimed < bytesToReclaim) {
          LOG.warning(name + " GC failed to reclaim sufficient disk space!");
        }
      }
    } catch (Throwable t) {
      LOG.log(Level.WARNING, name + " GC encountered an exception.", t);
    }
  }

  private static long fileSize(File f) {
    return f.isFile() ? f.length() : FileUtils.sizeOfDirectory(f);
  }

  private static final Comparator<File> LAST_MODIFIED_COMPARATOR = new Comparator<File>() {
    @Override public int compare(File file1, File file2) {
      long lastModified1 = recursiveLastModified(file1);
      long lastModified2 = recursiveLastModified(file2);

      if (lastModified1 < lastModified2) {
        return -1;
      } else if (lastModified1 > lastModified2) {
        return 1;
      } else {
        return 0;
      }
    }
  };
}
