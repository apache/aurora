package com.twitter.nexus.scheduler;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.twitter.nexus.util.HdfsUtil;
import org.apache.hadoop.fs.FileSystem;

import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

public class SchedulerModule extends AbstractModule {
  private final static Logger LOG = Logger.getLogger(SchedulerModule.class.getName());
  private SchedulerMain.TwitterSchedulerOptions options;

  @Inject
  public SchedulerModule(SchedulerMain.TwitterSchedulerOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void configure() {
    bind(SchedulerCore.class);
    bind(SchedulerHub.class);
  }

  @Provides
  @Singleton
  public FileSystem provideFileSystem() throws IOException {
    return HdfsUtil.getHdfsConfiguration(
        Preconditions.checkNotNull(options.hdfsConfig).getAbsolutePath());
  }
}
