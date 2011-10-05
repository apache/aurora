package com.twitter.mesos.executor;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import org.apache.hadoop.conf.Configuration;

import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common_internal.util.HdfsUtils;

/**
 * A file copier that will manage copies of files from HDFS to the local file system.
 *
 * @author William Farner
 */
public class HdfsFileCopier implements ExceptionalFunction<FileCopyRequest, File, IOException> {

  private final static Logger LOG = Logger.getLogger(HdfsFileCopier.class.getName());

  private final Configuration conf;

  @Inject
  public HdfsFileCopier(Configuration conf) {
    this.conf = Preconditions.checkNotNull(conf);
  }

  @Override
  public File apply(FileCopyRequest copy) throws IOException {
    LOG.info(String.format(
        "HDFS file %s -> local file %s", copy.getSourcePath(), copy.getDestPath()));
    try {
      return HdfsUtils.downloadFileFromHdfs(conf, copy.getSourcePath(), copy.getDestPath(), true);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to download file from HDFS", e);
      throw e;
    }
  }
}
