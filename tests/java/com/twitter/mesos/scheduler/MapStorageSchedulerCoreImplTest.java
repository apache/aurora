package com.twitter.mesos.scheduler;

import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import org.junit.Before;

import java.util.Set;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

/**
 * @author jsirois
 */
public class MapStorageSchedulerCoreImplTest extends BaseSchedulerCoreImplTest {

  private PersistenceLayer<NonVolatileSchedulerState> persistenceLayer;

  @Before
  public void mySetUp() {
    persistenceLayer = createMock(new Clazz<PersistenceLayer<NonVolatileSchedulerState>>() {});
  }

  @Override
  protected Storage createStorage(Set<JobManager> jobManagers) {
    return new MapStorage(persistenceLayer, jobManagers);
  }

  @Override
  protected void expectRestore() throws Exception {
    expect(persistenceLayer.fetch()).andReturn(null);
  }

  @Override
  protected void expectPersists(int count) throws Exception {
    persistenceLayer.commit((NonVolatileSchedulerState) anyObject());
    expectLastCall().times(count);
  }
}
