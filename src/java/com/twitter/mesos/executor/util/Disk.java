package com.twitter.mesos.executor.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Logger;

import org.apache.commons.io.FileUtils;

/**
 * Utility class for functions related to disk size.
 */
public class Disk {

  private static final Logger LOG = Logger.getLogger(Disk.class.getName());

  private Disk() {
    // Utility class.
  }

  /**
   * Calculates the size of a file, recursively.  If the file is a directory, the returned value
   * will be the size of all files underneath the directory.
   *
   * @param f File to calculate disk size for.
   * @return The recursive file size, in bytes.
   * @throws IOException If an error occurred while reading files.
   */
  public static long recursiveFileSize(File f) throws IOException {
    return f.isFile() ? f.length() : sizeOfDirectory(f);
  }

  // TODO(wfarner): Figure out a better long-term plan for the code below.  Right now, it is
  //     a shameless copy from org.apache.commons.io.FileUtils to quickly plug an issue caused
  //     by symlinks present in a directory.
  private static long sizeOf(File file) throws IOException {
    if (FileUtils.isSymlink(file)) {
      LOG.finest("Skipping symlink " + file);
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

  private static long sizeOfDirectory(File directory) throws IOException {
    if (FileUtils.isSymlink(directory)) {
      LOG.finest("Skipping symlink " + directory);
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
