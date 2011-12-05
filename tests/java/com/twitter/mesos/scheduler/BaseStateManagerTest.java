package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.testing.TearDown;

import org.apache.mesos.Protos.SlaveID;
import org.junit.Before;
import org.springframework.transaction.TransactionException;

import com.twitter.common.base.Closure;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.StateManagerVars.MutableState;
import com.twitter.mesos.scheduler.db.testing.DbStorageTestUtil;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Work.NoResult.Quiet;

/**
 * @author William Farner
 */
public abstract class BaseStateManagerTest extends EasyMockTest {

  protected Closure<String> killTaskCallback;
  protected StateManager stateManager;
  protected MutableState mutableState;
  protected FakeClock clock = new FakeClock();
  protected Storage storage;

  private int transactionsUntilFailure = 0;

  @Before
  public void stateManagerSetUp() throws Exception {
    killTaskCallback = createMock(new Clazz<Closure<String>>() {});
    stateManager = createStateManager();
  }

  protected StateManager createStateManager(final Storage wrappedStorage) {
    this.storage = new Storage() {
      @Override public void prepare() {
        wrappedStorage.prepare();
      }

      @Override public void start(Quiet initilizationLogic) {
        wrappedStorage.start(initilizationLogic);
      }

      @Override public <T, E extends Exception> T doInTransaction(final Work<T, E> work)
          throws StorageException, E {
        return wrappedStorage.doInTransaction(new Work<T, E>() {
          @Override public T apply(StoreProvider storeProvider) throws E {
            T result = work.apply(storeProvider);

            // Inject the failure after the work is performed in the transaction, so that we can
            // check for unintended side effects remaining.
            if ((transactionsUntilFailure != 0) && (--transactionsUntilFailure == 0)) {
              throw new TransactionException("Injected storage failure.") { };
            }
            return result;
          }
        });
      }

      @Override public void stop() {
        wrappedStorage.stop();
      }
    };
    this.mutableState = new MutableState();
    final StateManager stateManager = new StateManager(storage, clock, mutableState);
    stateManager.initialize();
    stateManager.start(killTaskCallback);
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        stateManager.stop();
      }
    });
    return stateManager;
  }

  protected void failNthTransaction(int n) {
    Preconditions.checkState(transactionsUntilFailure == 0, "Last failure has not yet occurred");
    transactionsUntilFailure = n;
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
