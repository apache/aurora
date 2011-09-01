package com.twitter.mesos.scheduler.log.db;

import java.io.File;

import com.google.inject.Binder;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.CanWrite;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.IsDirectory;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.scheduler.db.DbUtil;
import com.twitter.mesos.scheduler.log.Log;

/**
 * Bindings for the H2 simulated distributed log.
 *
 * @author John Sirois
 */
public class DbLogStreamModule extends PrivateModule {

  @NotNull
  @Exists
  @CanRead
  @CanWrite
  @IsDirectory
  @CmdLine(name = "dlog_db_file_path",
           help = "The path of the simulated dlog H2 db files.")
  private static final Arg<File> dbFilePath = Arg.create();

  @CmdLine(name = "dlog_db_cache_size",
           help = "The size to use for the simulated dlog H2 in-memory db cache.")
  private static final Arg<Amount<Long, Data>> dbCacheSize = Arg.create(Amount.of(256L, Data.MB));

  // TODO(John Sirois): reconsider exposing the db by default - obvious danger here
  @CmdLine(name = "dlog_db_admin_interface",
           help = "Turns on a web interface to the simulated dlog db - use with caution")
  private static final Arg<Boolean> exposeDbAdmin = Arg.create(true);

  /**
   * Binds a simulated distributed {@link Log} that uses a local H2 db.
   *
   * @param binder a guice binder to bind the simulated log with
   */
  public static void bind(Binder binder) {
    binder.install(new DbLogStreamModule());

    if (exposeDbAdmin.get()) {
      DbUtil.bindAdminInterface(binder, "/scheduler/dlog");
    }
  }

  @Override
  protected void configure() {
    bind(Log.class).to(DbLogStream.class).in(Singleton.class);

    DbUtil.fileSystem(dbFilePath.get(), "simulated-dlog", dbCacheSize.get())
        .secured("scheduler", "ep1nephrin3")
        .bind(binder());

    expose(Log.class);
  }
}
