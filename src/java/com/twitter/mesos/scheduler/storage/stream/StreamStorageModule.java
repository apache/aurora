package com.twitter.mesos.scheduler.storage.stream;

import java.io.File;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.scheduler.JobManager;
import com.twitter.mesos.scheduler.persistence.EncodingPersistenceLayer;
import com.twitter.mesos.scheduler.persistence.FileSystemPersistence;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import com.twitter.mesos.scheduler.persistence.ZooKeeperPersistence;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.StorageRoles;

/**
 * Provides bindings for stream based scheduler storage.
 *
 * @author John Sirois
 */
public class StreamStorageModule extends AbstractModule {

  @CmdLine(name = "scheduler_persistence_zookeeper_path",
          help ="Path in ZooKeeper that scheduler will persist to/from (overrides local).")
  private static final Arg<String> schedulerPersistenceZooKeeperPath = Arg.create(null);

  @CmdLine(name = "scheduler_persistence_zookeeper_version",
          help ="Version for scheduler persistence node in ZooKeeper.")
  private static final Arg<Integer> schedulerPersistenceZooKeeperVersion = Arg.create(1);

  @CmdLine(name = "scheduler_persistence_local_path",
          help ="Local file path that scheduler will persist to/from.")
  private static final Arg<File> schedulerPersistenceLocalPath =
      Arg.create(new File("/tmp/mesos_scheduler_dump"));

  private final Role storageRole;

  public StreamStorageModule(Role storageRole) {
    this.storageRole = Preconditions.checkNotNull(storageRole);
  }

  @Override
  protected void configure() {
    requireBinding(Key.get(new TypeLiteral<Set<JobManager>>() {}));

    bind(Storage.class).annotatedWith(StorageRoles.forRole(storageRole))
        .to(MapStorage.class).in(Singleton.class);
  }

  @Provides
  @Singleton
  final PersistenceLayer<NonVolatileSchedulerState> providePersistenceLayer(
      @Nullable ZooKeeperClient zkClient) {

    PersistenceLayer<byte[]> binaryPersistence;
    if (schedulerPersistenceZooKeeperPath.get() == null) {
      binaryPersistence = new FileSystemPersistence(schedulerPersistenceLocalPath.get());
    } else {
      if (zkClient == null) {
        throw new IllegalArgumentException(
            "ZooKeeper client must be available for ZooKeeper persistence layer.");
      }

      binaryPersistence = new ZooKeeperPersistence(zkClient,
          schedulerPersistenceZooKeeperPath.get(),
          schedulerPersistenceZooKeeperVersion.get());
    }

    return new EncodingPersistenceLayer(binaryPersistence);
  }
}
