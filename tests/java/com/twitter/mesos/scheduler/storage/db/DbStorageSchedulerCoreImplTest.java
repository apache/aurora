package com.twitter.mesos.scheduler.storage.db;

import java.sql.SQLException;

import com.twitter.mesos.scheduler.BaseSchedulerCoreImplTest;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.Storage;

public class DbStorageSchedulerCoreImplTest extends BaseSchedulerCoreImplTest {

  @Override
  protected Storage createStorage() throws SQLException {
    return DbStorageTestUtil.setupStorage(this);
  }
}
