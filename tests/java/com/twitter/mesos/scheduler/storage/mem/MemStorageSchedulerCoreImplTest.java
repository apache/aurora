package com.twitter.mesos.scheduler.storage.mem;

import com.twitter.mesos.scheduler.state.BaseSchedulerCoreImplTest;
import com.twitter.mesos.scheduler.storage.Storage;

public class MemStorageSchedulerCoreImplTest extends BaseSchedulerCoreImplTest {

  @Override
  protected Storage createStorage() {
    return MemStorage.newEmptyStorage();
  }
}
