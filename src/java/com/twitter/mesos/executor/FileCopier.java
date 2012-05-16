package com.twitter.mesos.executor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.twitter.common.base.ExceptionalFunction;

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
        return downloadFileFromHdfs(conf, copy.getSourcePath(), copy.getDestPath(),
            true /* overwrite */);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to download file from HDFS", e);
        throw new FileCopyException(e);
      } catch (NullPointerException e) {
        LOG.log(Level.SEVERE, "Failed to download file from HDFS", e);
        throw new FileCopyException(e);
      }
    }

    /**
     * A helper function to copy the file from HDFS. The code is directly copied from
     * HdfsUtils.downloadFileFromHdfs().
     * TODO(vinod): This is a temporary fix for MESOS-514.
     * Use HdfsUtils.downloadFileFromHdfs() instead, when smf1 is migrated to cdh3.
     */
    private static File downloadFileFromHdfs(Configuration conf, String fileUri,
        String localDirName, boolean overwrite) throws IOException {

      Path path = new Path(fileUri);
      FileSystem hdfs = path.getFileSystem(conf);
      FSDataInputStream remoteStream = hdfs.open(path);

      File localFile = new File(localDirName, path.getName());

      if (overwrite && localFile.exists()) {
        boolean success = localFile.delete();
        if (!success) {
          LOG.warning("Failed to delete file to be overwritten: " + localFile);
        }
      }

      FileOutputStream localStream = new FileOutputStream(localFile);
      try {
        IOUtils.copy(remoteStream, localStream);
      } finally {
        IOUtils.closeQuietly(remoteStream);
        IOUtils.closeQuietly(localStream);
      }
      return localFile;
    }
  }
}
