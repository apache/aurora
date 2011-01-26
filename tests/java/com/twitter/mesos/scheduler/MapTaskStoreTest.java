package com.twitter.mesos.scheduler;

/**
 * @author jsirois
 */
public class MapTaskStoreTest extends BaseTaskStoreTest<MapTaskStore> {
  @Override
  protected MapTaskStore createTaskStore() {
    return new MapTaskStore();
  }
}
