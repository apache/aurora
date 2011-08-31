package com.twitter.mesos.scheduler.storage;

import com.google.inject.AbstractModule;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.mesos.scheduler.storage.db.DbStorageModule;
import com.twitter.mesos.scheduler.storage.log.LogStorageModule;

/**
 * Binds storage used to persist scheduler state.
 *
 * @author John Sirois
 */
public class StorageModule extends AbstractModule {

  @CmdLine(name = "use_dlog",
           help ="Enables the distributed write-ahead log for storage HA.")
  private static final Arg<Boolean> useLog = Arg.create(false);

  @Override
  protected void configure() {
    if (useLog.get()) {
      LogStorageModule.bind(binder());
    } else {
      DbStorageModule.bind(binder());
    }
  }
}
