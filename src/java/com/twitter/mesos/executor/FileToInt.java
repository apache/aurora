package com.twitter.mesos.executor;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.mesos.executor.FileToInt.FetchException;

import java.io.File;
import java.io.IOException;

/**
 * A function to read a file and interpret its contents as an integer.
 *
 * @author wfarner
 */
public class FileToInt implements ExceptionalFunction<File, Integer, FetchException> {
  @Override
  public Integer apply(File file) throws FetchException {
    Preconditions.checkNotNull(file);
    Preconditions.checkArgument(file.canRead());

    try {
      return Integer.parseInt(Joiner.on("").join(Files.readLines(file, Charsets.UTF_8)));
    } catch (IOException e) {
      throw new FetchException("Failed to read file " + file, e);
    } catch (NumberFormatException e) {
      throw new FetchException("File contents not an integer " + file, e);
    }
  }

  public static class FetchException extends Exception {
    public FetchException(String msg) {
      super(msg);
    }
    public FetchException(String msg, Throwable t) {
      super(msg, t);
    }
  }
}
