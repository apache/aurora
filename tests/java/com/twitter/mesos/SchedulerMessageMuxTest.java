package com.twitter.mesos;

import com.google.common.collect.Maps;
import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.LiveTaskInfo;
import com.twitter.mesos.gen.RegisteredTaskUpdate;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.SchedulerMessage;
import com.twitter.mesos.gen.SchedulerMessageType;
import org.apache.thrift.TBase;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author wfarner
 */
public class SchedulerMessageMuxTest {

  private IMocksControl control;

  private Map<SchedulerMessageType, Closure<TBase>> callbacks = Maps.newHashMap();

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    control = createControl();

    for (SchedulerMessageType type : SchedulerMessageType.values()) {
      callbacks.put(type, control.createMock(Closure.class));
    }
  }

  @After
  public void checkControl() {
    control.verify();
  }

  @Test
  public void testHappyCase() throws Exception {
    RegisteredTaskUpdate update = new RegisteredTaskUpdate();
    update.setSlaveHost("test");
    update.setTaskInfos(Arrays.asList(new LiveTaskInfo().setStatus(ScheduleStatus.PENDING)));

    callbacks.get(SchedulerMessageType.REGISTERED_TASK_UPDATE).execute(update);

    control.replay();

    SchedulerMessage message = SchedulerMessageMux.mux(SchedulerMessageType.REGISTERED_TASK_UPDATE,
        RegisteredTaskUpdate.class, update);
    SchedulerMessageMux.demux(message, callbacks);
  }

  @Test
  public void testAllTypesMapped() {
    control.replay();

    for (SchedulerMessageType type : SchedulerMessageType.values()) {
      assertNotNull(SchedulerMessageMux.TYPE_MAPPING.get(type));
    }
  }
}
