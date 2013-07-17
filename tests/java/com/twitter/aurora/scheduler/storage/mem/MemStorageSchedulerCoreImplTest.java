package com.twitter.aurora.scheduler.storage.mem;

import com.twitter.aurora.scheduler.state.BaseSchedulerCoreImplTest;
import com.twitter.aurora.scheduler.storage.Storage;

public class MemStorageSchedulerCoreImplTest extends BaseSchedulerCoreImplTest {

  @Override
  protected Storage createStorage() {
    return MemStorage.newEmptyStorage();
  }
}
