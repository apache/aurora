package com.twitter.mesos.scheduler;

import com.twitter.mesos.scheduler.DbStorage.Configuration;

import java.sql.SQLException;
import java.util.Set;

/**
 * @author jsirois
 */
public class DbStorageSchedulerCoreImplTest extends BaseSchedulerCoreImplTest {

  @Override
  protected Storage createStorage(Set<JobManager> jobManagers) throws SQLException {
    return DbStorageTestUtil.setupStorage(this, new Configuration());
  }

  @Override
  protected void expectRestore() throws Exception {
    // noop
  }

  @Override
  protected void expectPersists(int count) throws Exception {
    // noop
  }
}
