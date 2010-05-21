package com.twitter.nexus.executor;

import com.twitter.common.base.MorePreconditions;

/**
 * A bean to contain the state necessary to complete a file copy.
 *
 * @author wfarner
 */
public class FileCopyRequest {
  private final String sourcePath;
  private final String destPath;

  public FileCopyRequest(String sourcePath, String destPath) {
    this.sourcePath = MorePreconditions.checkNotBlank(sourcePath);
    this.destPath = MorePreconditions.checkNotBlank(destPath);
  }

  public String getSourcePath() {
    return sourcePath;
  }

  public String getDestPath() {
    return destPath;
  }
}
