package com.twitter.mesos.scheduler.storage.log;

import java.lang.annotation.Annotation;

import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateBinder;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.scheduler.log.mesos.MesosLogStreamModule;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;
import com.twitter.mesos.scheduler.storage.db.DbStorage;
import com.twitter.mesos.scheduler.storage.db.DbStorageModule;
import com.twitter.mesos.scheduler.storage.log.LogManager.MaxEntrySize;
import com.twitter.mesos.scheduler.storage.log.LogStorage.ShutdownGracePeriod;
import com.twitter.mesos.scheduler.storage.log.LogStorage.SnapshotInterval;


/**
 * Bindings for scheduler distributed log based storage.
 *
 * @author John Sirois
 */
public class LogStorageModule extends AbstractModule {

  @CmdLine(name = "dlog_shutdown_grace_period",
           help = "Specifies the maximum time to wait for scheduled checkpoint and snapshot "
                  + "actions to complete before forcibly shutting down.")
  private static final Arg<Amount<Long, Time>> shutdownGracePeriod =
      Arg.create(Amount.of(2L, Time.SECONDS));

  @CmdLine(name = "dlog_snapshot_interval",
           help = "Specifies the frequency at which snapshots of local storage are taken and "
                  + "written to the log.")
  private static final Arg<Amount<Long, Time>> snapshotInterval =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "dlog_max_entry_size",
           help = "Specifies the maximum entry size to append to the log. Larger entries will be "
                  + "split across entry Frames.")
  private static final Arg<Amount<Integer, Data>> maxLogEntrySize =
      Arg.create(Amount.of(512, Data.KB));

  private static <T> Key<T> createKey(Class<T> clazz) {
    return Key.get(clazz, LogStorage.WriteBehind.class);
  }

  /**
   * Binds a distributed log based storage system.
   *
   * @param binder a guice binder to bind the storage with
   */
  public static void bind(Binder binder) {
    // TODO(John Sirois): parameterize H2 bindings to accept a mem configuration once we have
    // the core log exposing its listener interface.  No need to store twice.
    DbStorageModule.bind(binder, LogStorage.WriteBehind.class, new Closure<PrivateBinder>() {
      @Override public void execute(PrivateBinder binder) {
        binder.bind(new TypeLiteral<SnapshotStore<byte[]>>() {}).to(DbStorage.class);

        TypeLiteral<SnapshotStore<Snapshot>> snapshotStoreType =
            new TypeLiteral<SnapshotStore<Snapshot>>() {};
        binder.bind(snapshotStoreType).to(SnapshotStoreImpl.class);
        binder.expose(snapshotStoreType);

        Key<SchedulerStore> SCHEDULER_STORE_KEY = createKey(SchedulerStore.class);
        binder.bind(SCHEDULER_STORE_KEY).to(DbStorage.class);
        binder.expose(SCHEDULER_STORE_KEY);

        Key<JobStore> JOB_STORE_KEY = createKey(JobStore.class);
        binder.bind(JOB_STORE_KEY).to(DbStorage.class);
        binder.expose(JOB_STORE_KEY);

        Key<TaskStore> TASK_STORE_KEY = createKey(TaskStore.class);
        binder.bind(TASK_STORE_KEY).to(DbStorage.class);
        binder.expose(TASK_STORE_KEY);

        Key<UpdateStore> UPDATE_STORE_KEY = createKey(UpdateStore.class);
        binder.bind(UPDATE_STORE_KEY).to(DbStorage.class);
        binder.expose(UPDATE_STORE_KEY);

        Key<QuotaStore> QUOTA_STORE_KEY = createKey(QuotaStore.class);
        binder.bind(QUOTA_STORE_KEY).to(DbStorage.class);
        binder.expose(QUOTA_STORE_KEY);
      }
    });

    MesosLogStreamModule.bind(binder);
    binder.install(new LogStorageModule());
  }

  @Override
  protected void configure() {
    requireBinding(Clock.class);
    requireBinding(ShutdownRegistry.class);

    bindInterval(ShutdownGracePeriod.class, shutdownGracePeriod);
    bindInterval(SnapshotInterval.class, snapshotInterval);

    bind(new TypeLiteral<Amount<Integer, Data>>() {}).annotatedWith(MaxEntrySize.class)
        .toInstance(maxLogEntrySize.get());
    bind(LogManager.class).in(Singleton.class);

    bind(Storage.class).to(LogStorage.class);
    bind(LogStorage.class).in(Singleton.class);
  }

  private void bindInterval(Class<? extends Annotation> key, Arg<Amount<Long, Time>> value) {
    bind(Key.get(new TypeLiteral<Amount<Long, Time>>() {}, key)).toInstance(value.get());
  }
}
