package com.twitter.mesos.util;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
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

  public static Configuration getHdfsConfiguration(String hdfsConfigPath) throws IOException {
    Configuration conf = new Configuration(true);
    conf.addResource(new Path(hdfsConfigPath));
    conf.reloadConfiguration();
    return conf;
  }

  public static File downloadFileFromHdfs(Configuration conf, String fileUri,
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

  public static void main(String[] args) throws IOException {
    if (args.length < 2 || args.length > 3) {
      System.err.printf("java %s [hdfs config file] [hdfs from] ([local to])",
          HdfsUtil.class.getName());
      System.err.println();
      System.exit(1);
    }
    Configuration conf = HdfsUtil.getHdfsConfiguration(args[0]);
    HdfsUtil.downloadFileFromHdfs(conf, args[1], ((args.length == 3) ? args[2] : "."), true);
  }
}
