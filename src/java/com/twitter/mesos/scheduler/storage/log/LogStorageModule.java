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
import com.twitter.mesos.scheduler.db.DbUtil.Builder.JdbcUrl;
import com.twitter.mesos.scheduler.log.Log;
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
 * <p/>
 * Requires bindings for:
 * <ul>
 *   <li>{@link Clock}</li>
 *   <li>{@link ShutdownRegistry}</li>
 *   <li>The concrete {@link Log} implementation.</li>
 * </ul>
 * <p/>
 * Exposes bindings for storage components:
 * <ul>
 *   <li>jdbc URL keyed by {@link JdbcUrl}</li>
 *   <li>{@link Storage}</li>
 *   <li>{@link SnapshotStore}</li>
 *   <li>Keyed by {@link LogStorage.WriteBehind}
 *     <ul>
 *       <li>{@link SchedulerStore}</li>
 *       <li>{@link JobStore}</li>
 *       <li>{@link TaskStore}</li>
 *       <li>{@link UpdateStore}</li>
 *       <li>{@link QuotaStore}</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class LogStorageModule extends AbstractModule {

  @CmdLine(name = "dlog_shutdown_grace_period",
           help = "Specifies the maximum time to wait for scheduled checkpoint and snapshot "
                  + "actions to complete before forcibly shutting down.")
  private static final Arg<Amount<Long, Time>> SHUTDOWN_GRACE_PERIOD =
      Arg.create(Amount.of(2L, Time.SECONDS));

  @CmdLine(name = "dlog_snapshot_interval",
           help = "Specifies the frequency at which snapshots of local storage are taken and "
                  + "written to the log.")
  private static final Arg<Amount<Long, Time>> SNAPSHOT_INTERVAL =
      Arg.create(Amount.of(5L, Time.MINUTES));

  @CmdLine(name = "dlog_max_entry_size",
           help = "Specifies the maximum entry size to append to the log. Larger entries will be "
                  + "split across entry Frames.")
  private static final Arg<Amount<Integer, Data>> MAX_LOG_ENTRY_SIZE =
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
    DbStorageModule.bind(binder, LogStorage.WriteBehind.class, new Closure<PrivateBinder>() {
      @Override public void execute(PrivateBinder binder) {
        binder.bind(new TypeLiteral<SnapshotStore<byte[]>>() { }).to(DbStorage.class);

        TypeLiteral<SnapshotStore<Snapshot>> snapshotStoreType =
            new TypeLiteral<SnapshotStore<Snapshot>>() { };
        binder.bind(snapshotStoreType).to(SnapshotStoreImpl.class);
        binder.expose(snapshotStoreType);

        Key<SchedulerStore.Mutable> schedulerStoreKey = createKey(SchedulerStore.Mutable.class);
        binder.bind(schedulerStoreKey).to(DbStorage.class);
        binder.expose(schedulerStoreKey);

        Key<JobStore.Mutable> jobStoreKey = createKey(JobStore.Mutable.class);
        binder.bind(jobStoreKey).to(DbStorage.class);
        binder.expose(jobStoreKey);

        Key<TaskStore.Mutable> taskStoreKey = createKey(TaskStore.Mutable.class);
        binder.bind(taskStoreKey).to(DbStorage.class);
        binder.expose(taskStoreKey);

        Key<UpdateStore.Mutable> updateStoreKey = createKey(UpdateStore.Mutable.class);
        binder.bind(updateStoreKey).to(DbStorage.class);
        binder.expose(updateStoreKey);

        Key<QuotaStore.Mutable> quotaStoreKey = createKey(QuotaStore.Mutable.class);
        binder.bind(quotaStoreKey).to(DbStorage.class);
        binder.expose(quotaStoreKey);

        // Expose the jdbc url for tools
        binder.expose(Key.get(String.class, JdbcUrl.class));
      }
    });

    binder.install(new LogStorageModule());
  }

  @Override
  protected void configure() {
    requireBinding(Log.class);
    requireBinding(Clock.class);
    requireBinding(ShutdownRegistry.class);

    bindInterval(ShutdownGracePeriod.class, SHUTDOWN_GRACE_PERIOD);
    bindInterval(SnapshotInterval.class, SNAPSHOT_INTERVAL);

    bind(new TypeLiteral<Amount<Integer, Data>>() { }).annotatedWith(MaxEntrySize.class)
        .toInstance(MAX_LOG_ENTRY_SIZE.get());
    bind(LogManager.class).in(Singleton.class);

    bind(Storage.class).to(LogStorage.class);
    bind(LogStorage.class).in(Singleton.class);
  }

  private void bindInterval(Class<? extends Annotation> key, Arg<Amount<Long, Time>> value) {
    bind(Key.get(new TypeLiteral<Amount<Long, Time>>() { }, key)).toInstance(value.get());
  }
}
