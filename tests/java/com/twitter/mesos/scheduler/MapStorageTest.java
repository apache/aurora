package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.base.Commands;
import com.twitter.common.testing.EasyMockTest.Clazz;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;

/**
 * @author jsirois
 */
public class MapStorageTest extends BaseTaskStoreTest<TaskStore> {
  @Override
  protected TaskStore createTaskStore() {
    PersistenceLayer<NonVolatileSchedulerState> persistenceLayer =
        new Clazz<PersistenceLayer<NonVolatileSchedulerState>>() {}.createMock();

    MapStorage store = new MapStorage(persistenceLayer, ImmutableSet.<JobManager>of());
    return store.asTaskStore(Commands.NOOP);
  }
}
