package com.twitter.nexus.executor;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.internal.Preconditions;
import com.twitter.nexus.util.HdfsUtil;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * ExecutorModule
 *
 * @author Florian Leibert
 */
public class ExecutorModule extends AbstractModule {
  private final static java.util.logging.Logger LOG = Logger.getLogger(ExecutorModule.class.getName());
  private final ExecutorMain.TwitterExecutorOptions options;

  @Inject
  public ExecutorModule(ExecutorMain.TwitterExecutorOptions options) {
    this.options = Preconditions.checkNotNull(options);
  }

  @Override
  protected void configure() {
    bind(ExecutorCore.class);
    bind(ExecutorHub.class);
  }

  @Provides
  @Singleton
  public FileSystem provideFileSystem() throws IOException {
   return HdfsUtil.getHdfsConfiguration(options.hdfsConfig);
  }
}
