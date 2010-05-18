package com.twitter.nexus.scheduler.persistence;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Persistence layer that stores data on the local file system.
 *
 * @author wfarner
 */
public class FileSystemPersistence implements PersistenceLayer {
  private final static Logger LOG = Logger.getLogger(FileSystemPersistence.class.getName());

  private final File file;

  public FileSystemPersistence(File file) {
    this.file = Preconditions.checkNotNull(file);
  }

  @Override
  public byte[] fetch() throws PersistenceException {
    try {
      return Files.toByteArray(file);
    } catch (IOException e) {
      logAndThrow("Failed to read file " + file.getAbsolutePath(), e);
    }

    return null;
  }

  @Override
  public void commit(byte[] data) throws PersistenceException {
    try {
      Files.write(data, file);
    } catch (IOException e) {
      logAndThrow("Failed to write to file " + file.getAbsolutePath(), e);
    }
  }

  private static void logAndThrow(String msg, Throwable t) throws PersistenceException {
    LOG.log(Level.SEVERE, msg, t);
    throw new PersistenceException(msg, t);
  }
}
