package com.twitter.nexus.util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * HdfsUtil - utility functions for dealing with Hadoop's file system
 * 
 * @author Florian Leibert
 */
public class HdfsUtil {
  private final static java.util.logging.Logger LOG = Logger.getLogger(HdfsUtil.class.getName());

  public static FileSystem getHdfsConfiguration(final String hdfsConfigPath) throws IOException {
    Configuration conf = new Configuration(true);
    conf.addResource(new Path(hdfsConfigPath));
    conf.reloadConfiguration();
    FileSystem hdfs = FileSystem.get(conf);
    return hdfs;
  }

  public static File downloadFileFromHdfs(final FileSystem hdfs, final String executorBinaryUrl,
                                          final String localDirName) throws IOException {
    Path executorBinaryPath = new Path(executorBinaryUrl);
    FSDataInputStream remoteStream = hdfs.open(executorBinaryPath);
    File localFile = new File(localDirName + executorBinaryPath.getName());
    FileOutputStream localStream = new FileOutputStream(localFile);
    try {
      IOUtils.copy(remoteStream, localStream);
    } finally {
      IOUtils.closeQuietly(remoteStream);
      IOUtils.closeQuietly(localStream);
    }
    return localFile;
  }

  public static boolean isValidFile(final FileSystem hdfs, final String path) throws IOException {
    FileStatus[] statuses = hdfs.listStatus(new Path(path));
    return (statuses.length == 1 && !statuses[0].isDir() && statuses[0].getBlockSize() > 0L);
  }
}
