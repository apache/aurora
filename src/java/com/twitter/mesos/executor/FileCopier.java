package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation.HadoopLoginModule;

import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.base.ExceptionalSupplier;
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

      return doWithHadoopLoginFix(new ExceptionalSupplier<File, FileCopyException>() {
        @Override public File get() throws FileCopyException {
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
      });
    }

    private static <T, X extends Exception> T doWithHadoopLoginFix(ExceptionalSupplier<T, X> work)
        throws X {

      // Temporarily switch the thread's ContextClassLoader to match the hadoop classloader
      // so that it can properly load HadoopLoginModule from the JAAS libraries.  This is fixed in
      // hadoop-core 0.23.1 : https://issues.apache.org/jira/browse/HADOOP-7982
      Thread currentThread = Thread.currentThread();
      ClassLoader prior = currentThread.getContextClassLoader();
      try {
        currentThread.setContextClassLoader(HadoopLoginModule.class.getClassLoader());
        return work.get();
      } finally {
        currentThread.setContextClassLoader(prior);
      }
    }
  }
}
