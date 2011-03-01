package com.twitter.mesos.scheduler.storage.stream;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.scheduler.JobManager;
import com.twitter.mesos.scheduler.SchedulerMain.TwitterSchedulerOptions;
import com.twitter.mesos.scheduler.persistence.EncodingPersistenceLayer;
import com.twitter.mesos.scheduler.persistence.FileSystemPersistence;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import com.twitter.mesos.scheduler.persistence.ZooKeeperPersistence;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.StorageRoles;

import java.util.Set;
import javax.annotation.Nullable;

/**
 * Provides bindings for stream based scheduler storage.
 *
 * @author John Sirois
 */
public class StreamStorageModule extends AbstractModule {

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
      TwitterSchedulerOptions options, @Nullable ZooKeeperClient zkClient) {

    PersistenceLayer<byte[]> binaryPersistence;
    if (options.schedulerPersistenceZooKeeperPath == null) {
      binaryPersistence = new FileSystemPersistence(options.schedulerPersistenceLocalPath);
    } else {
      if (zkClient == null) {
        throw new IllegalArgumentException(
            "ZooKeeper client must be available for ZooKeeper persistence layer.");
      }

      binaryPersistence = new ZooKeeperPersistence(zkClient,
          options.schedulerPersistenceZooKeeperPath,
          options.schedulerPersistenceZooKeeperVersion);
    }

    return new EncodingPersistenceLayer(binaryPersistence);
  }
}
