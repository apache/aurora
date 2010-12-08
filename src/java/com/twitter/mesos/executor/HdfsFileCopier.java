package com.twitter.mesos.executor;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.mesos.util.HdfsUtil;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * A file copier that will manage copies of files from HDFS to the local file system.
 *
 * @author wfarner
 */
public class HdfsFileCopier implements ExceptionalFunction<FileCopyRequest, File, IOException> {

  private final static Logger LOG = Logger.getLogger(HdfsFileCopier.class.getName());

  private final FileSystem fileSystem;

  @Inject
  public HdfsFileCopier(FileSystem fileSystem) {
    this.fileSystem = Preconditions.checkNotNull(fileSystem);
  }

  @Override
  public File apply(FileCopyRequest copy) throws IOException {
    LOG.info(String.format(
        "HDFS file %s -> local file %s", copy.getSourcePath(), copy.getDestPath()));
    // Thanks, Apache, for writing good code and just assuming that the path i give you has
    // a trailing slash.  Of course it makes sense to blindly append a file name to the path
    // i provide.
    String dirWithSlash = copy.getDestPath();
    if (!dirWithSlash.endsWith("/")) dirWithSlash += "/";

    return HdfsUtil.downloadFileFromHdfs(fileSystem, copy.getSourcePath(), dirWithSlash, true);
  }
}
