package com.twitter.mesos.updater;

import com.google.common.collect.ImmutableMap;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.MesosSchedulerManager.Iface;
import com.twitter.mesos.gen.ResponseCode;
import com.twitter.mesos.gen.UpdateCompleteResponse;
import com.twitter.mesos.gen.UpdateConfig;
import com.twitter.mesos.gen.UpdateConfigResponse;
import com.twitter.mesos.updater.ConfigParser.UpdateConfigException;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.fail;

/**
 * To change this template use File | Settings | File Templates.
 *
 * @author wfarner
 */
public class CoordinatorTest extends EasyMockTest {

  private static final String UPDATE_TOKEN = "token";

  private Iface scheduler;

  @Before
  public void setUp() {
    scheduler = createMock(Iface.class);
  }

  @Test
  public void testBadConfig() throws Exception {
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("canary_size", "asdf")));
    expectFinished();
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("canary_size", "-1")));
    expectFinished();
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("canary_size", "0")));
    expectFinished();
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("tolerated_canary_failures", "-1")));
    expectFinished();
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("canary_watch_secs", "0")));
    expectFinished();
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("update_batch_size", "0")));
    expectFinished();
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("tolerated_total_failures", "-1")));
    expectFinished();
    expectGetConfig().andReturn(makeConfig(ImmutableMap.of("update_watch_secs", "0")));
    expectFinished();

    control.replay();

    expectBadConfig();
    expectBadConfig();
    expectBadConfig();
    expectBadConfig();
    expectBadConfig();
    expectBadConfig();
    expectBadConfig();
    expectBadConfig();
  }

  private IExpectationSetters<UpdateConfigResponse> expectGetConfig() throws Exception {
    return expect(scheduler.getUpdateConfig(UPDATE_TOKEN));
  }

  private void expectFinished() throws Exception {
    expect(scheduler.finishUpdate(UPDATE_TOKEN))
        .andReturn(new UpdateCompleteResponse(ResponseCode.OK, "OK"));
  }

  private void expectBadConfig() throws Exception {
    Coordinator coordinator = new Coordinator(scheduler, UPDATE_TOKEN);
    try {
      coordinator.run();
      fail("Bad config allowed.");
    } catch (UpdateConfigException e) {
      // Expected.
    }
  }

  private UpdateConfigResponse makeConfig(Map<String, String> params) {
    return new UpdateConfigResponse()
        .setResponseCode(ResponseCode.OK)
        .setConfig(new UpdateConfig().setConfig(params));
  }
}
