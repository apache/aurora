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
  public void mySetUp() throws Exception {
    persistenceLayer = createMock(new Clazz<PersistenceLayer<NonVolatileSchedulerState>>() {});

    expect(persistenceLayer.fetch()).andReturn(null).times(2);
    persistenceLayer.commit((NonVolatileSchedulerState) anyObject());
    expectLastCall().anyTimes();
  }

  @Override
  protected Storage createStorage() {
    return new MapStorage(persistenceLayer);
  }
}
