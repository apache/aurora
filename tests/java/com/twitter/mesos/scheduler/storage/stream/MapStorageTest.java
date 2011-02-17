package com.twitter.mesos.scheduler.storage.stream;

import com.twitter.common.base.Commands;
import com.twitter.common.testing.EasyMockTest.Clazz;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import com.twitter.mesos.scheduler.storage.BaseTaskStoreTest;
import com.twitter.mesos.scheduler.storage.TaskStore;

/**
 * @author jsirois
 */
public class MapStorageTest extends BaseTaskStoreTest<TaskStore> {
  @Override
  protected TaskStore createTaskStore() {
    PersistenceLayer<NonVolatileSchedulerState> persistenceLayer =
        new Clazz<PersistenceLayer<NonVolatileSchedulerState>>() {}.createMock();

    MapStorage store = new MapStorage(persistenceLayer);
    return store.asTaskStore(Commands.NOOP);
  }
}
