package com.twitter.mesos.scheduler.storage.stream;

import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.scheduler.BaseSchedulerCoreImplTest;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import com.twitter.mesos.scheduler.storage.Storage;
import org.junit.Before;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

/**
 * @author John Sirois
 */
public class MapStorageSchedulerCoreImplTest extends BaseSchedulerCoreImplTest {

  private PersistenceLayer<NonVolatileSchedulerState> persistenceLayer;

  @Before
  public void mySetUp() {
    persistenceLayer = createMock(new Clazz<PersistenceLayer<NonVolatileSchedulerState>>() {});
  }

  @Override
  protected Storage createStorage() {
    return new MapStorage(persistenceLayer);
  }

  @Override
  protected void expectRestore() throws Exception {
    expect(persistenceLayer.fetch()).andReturn(null).times(2);
  }

  @Override
  protected void expectPersists(int count) throws Exception {
    persistenceLayer.commit((NonVolatileSchedulerState) anyObject());
    expectLastCall().times(count);
  }
}
