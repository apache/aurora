package com.twitter.mesos.scheduler.persistence;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.twitter.common.stats.Stats;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Persistence layer that stores data on the local file system.
 *
 * @author William Farner
 */
public class FileSystemPersistence implements PersistenceLayer<byte[]> {
  private final static Logger LOG = Logger.getLogger(FileSystemPersistence.class.getName());

  private final File file;
  final AtomicLong persistedStateBytes = Stats.exportLong("persisted_state_bytes");

  public FileSystemPersistence(File file) {
    this.file = Preconditions.checkNotNull(file);
  }

  @Override
  public byte[] fetch() throws PersistenceException {
    LOG.info("Fetching persisted state from " + file.getAbsolutePath());
    try {
      byte[] data = Files.toByteArray(file);
      LOG.info("Fetched " + data.length + " bytes.");
      return data;
    } catch (IOException e) {
      logAndThrow("Failed to read file " + file.getAbsolutePath(), e);
    }

    return null;
  }

  @Override
  public void commit(byte[] data) throws PersistenceException {
    LOG.info("Storing " + data.length + " bytes.");

    if (!file.exists()) {
      File parent = file.getParentFile();
      if (!parent.exists() && !parent.mkdirs()) {
        throw new PersistenceException("Failed to create persistence directory: "
                                       + file.getParentFile().getAbsolutePath());
      }
    }
    try {
      File tempFile = new File(file.getAbsolutePath() + ".tmp");
      Files.write(data, tempFile);
      Files.move(tempFile, file);
      persistedStateBytes.set(data.length);
      tempFile.delete();
    } catch (IOException e) {
      logAndThrow("Failed to write to file " + file.getAbsolutePath(), e);
    }
  }

  private static void logAndThrow(String msg, Throwable t) throws PersistenceException {
    LOG.log(Level.SEVERE, msg, t);
    throw new PersistenceException(msg, t);
  }
}
