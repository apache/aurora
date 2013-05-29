package com.twitter.mesos.scheduler.thrift;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.SessionKey;
import com.twitter.mesos.gen.StartUpdateResponse;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

public class FeatureToggleSchedulerControllerTest extends EasyMockTest {

  private static final SessionKey SESSION = new SessionKey();
  private static final JobConfiguration CONFIG = new JobConfiguration();

  private SchedulerController delegate;

  @Before
  public void setUp() {
    delegate = createMock(SchedulerController.class);
  }

  @Test
  public void testUpdatesEnabled() {
    SchedulerController toggle = new FeatureToggleSchedulerController(delegate, true);
    StartUpdateResponse response = new StartUpdateResponse();
    expect(delegate.startUpdate(CONFIG, SESSION)).andReturn(response);

    control.replay();

    assertSame(response, toggle.startUpdate(CONFIG, SESSION));
  }

  @Test
  public void testUpdatesDisabled() {
    SchedulerController toggle = new FeatureToggleSchedulerController(delegate, false);

    control.replay();

    StartUpdateResponse response = toggle.startUpdate(CONFIG, SESSION);
    assertEquals(ResponseCode.ERROR, response.getResponseCode());
  }
}
