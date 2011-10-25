package com.twitter.mesos.scheduler;

import com.google.common.collect.ImmutableSet;
import com.google.common.testing.TearDown;
import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.Storage;

import org.apache.mesos.Protos.SlaveID;
import org.junit.Before;

/**
 * @author William Farner
 */
public abstract class BaseStateManagerTest extends EasyMockTest {

  protected Closure<String> killTaskCallback;
  protected StateManager stateManager;
  protected FakeClock clock = new FakeClock();
  protected Storage storage;

  @Before
  public void stateManagerSetUp() throws Exception {
    killTaskCallback = createMock(new Clazz<Closure<String>>() {});
    stateManager = createStateManager();
  }

  protected StateManager createStateManager(Storage storage) {
    this.storage = storage;
    final StateManager stateManager = new StateManager(storage, clock);
    stateManager.initialize();
    stateManager.start(killTaskCallback);
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        stateManager.stop();
      }
    });
    return stateManager;
  }

  private StateManager createStateManager() throws Exception {
    return createStateManager(createStorage());
  }

  protected Storage createStorage() throws Exception {
    return DbStorageTestUtil.setupStorage(this);
  }

  protected static TwitterTaskInfo makeTask(String owner, String job, int shard) {
    return new TwitterTaskInfo()
        .setOwner(new Identity().setRole(owner).setUser(owner))
        .setJobName(job)
        .setShardId(shard)
        .setStartCommand("echo");
  }

  protected void assignTask(String taskId, String host) {
    stateManager.assignTask(taskId, host, SlaveID.newBuilder().setValue(host).build(),
        ImmutableSet.<Integer>of());
  }
}
