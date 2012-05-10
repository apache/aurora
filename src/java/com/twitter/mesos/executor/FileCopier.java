package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.hadoop.conf.Configuration;

import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common_internal.hadoop.HdfsUtils;

import static com.twitter.mesos.executor.FileCopier.FileCopyException;

/**
 * A function that copies a file.
 *
 * @author William Farner
 */
public interface FileCopier extends ExceptionalFunction<FileCopyRequest, File, FileCopyException> {

  /**
   * Thrown when a file copy request fails either due to an IOException or a NullPointerException.
   */
  public static class FileCopyException extends Exception {
    public FileCopyException(Throwable cause) {
      super(cause);
    }
  }

  /**
   * A file copier that will manage copies of files from HDFS to the local file system.
   *
   * @author William Farner
   */
  static class HdfsFileCopier implements FileCopier {

    private final static Logger LOG = Logger.getLogger(FileCopier.class.getName());

    private final Configuration conf;

    @Inject
    public HdfsFileCopier(Configuration conf) {
      this.conf = Preconditions.checkNotNull(conf);
    }

    @Override
    public File apply(final FileCopyRequest copy) throws FileCopyException {
      LOG.info(String.format(
          "HDFS file %s -> local file %s", copy.getSourcePath(), copy.getDestPath()));

      try {
        return HdfsUtils.downloadFileFromHdfs(conf, copy.getSourcePath(), copy.getDestPath(),
            true /* overwrite */);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to download file from HDFS", e);
        throw new FileCopyException(e);
      } catch (NullPointerException e) {
        LOG.log(Level.SEVERE, "Failed to download file from HDFS", e);
        throw new FileCopyException(e);
      }
    }
  }
}
