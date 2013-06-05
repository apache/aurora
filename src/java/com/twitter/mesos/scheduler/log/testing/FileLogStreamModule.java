package com.twitter.mesos.scheduler.log.testing;

import java.io.File;

import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.mesos.scheduler.log.Log;

/**
 * Binding module that uses a local log file, intended for testing.
 */
public class FileLogStreamModule extends PrivateModule {

  @NotNull
  @CmdLine(name = "testing_log_file_path", help = "Path to a file to store local log file data in.")
  private static final Arg<File> LOG_PATH = Arg.create(null);

  @Override
  protected void configure() {
    bind(File.class).toInstance(LOG_PATH.get());
    bind(Log.class).to(FileLog.class);
    bind(FileLog.class).in(Singleton.class);
    expose(Log.class);
  }
}
