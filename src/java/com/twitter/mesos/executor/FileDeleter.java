package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.twitter.common.base.ExceptionalClosure;

/**
 * Abstraction for deleting files from the file system.
 *
 * @author William Farner
 */
public interface FileDeleter extends ExceptionalClosure<File, IOException> {

  /**
   * File deleter that recursively deletes files from the file system.
   */
  class FileDeleterImpl implements FileDeleter {
    @Override public void execute(File file) throws IOException {
      if (file.isFile()) {
        file.delete();
      } else {
        FileUtils.deleteDirectory(file);
      }
    }
  }
}
