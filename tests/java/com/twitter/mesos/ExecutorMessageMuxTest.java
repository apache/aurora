package com.twitter.mesos;

import com.google.common.collect.Maps;
import com.twitter.common.base.Closure;
import com.twitter.mesos.gen.MachineDrain;
import com.twitter.mesos.gen.ExecutorMessage;
import com.twitter.mesos.gen.ExecutorMessageType;
import org.apache.thrift.TBase;
import org.easymock.IMocksControl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.easymock.EasyMock.*;
import static org.junit.Assert.*;

/**
 * @author wfarner
 */
public class ExecutorMessageMuxTest {

  private IMocksControl control;

  private Map<ExecutorMessageType, Closure<TBase>> callbacks = Maps.newHashMap();

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() {
    control = createControl();

    for (ExecutorMessageType type : ExecutorMessageType.values()) {
      callbacks.put(type, control.createMock(Closure.class));
    }
  }

  @After
  public void checkControl() {
    control.verify();
  }

  @Test
  public void testHappyCase() throws Exception {
    MachineDrain machineDrain = new MachineDrain();

    callbacks.get(ExecutorMessageType.MACHINE_DRAIN).execute(machineDrain);

    control.replay();

    ExecutorMessage message = ExecutorMessageMux.mux(ExecutorMessageType.MACHINE_DRAIN,
        MachineDrain.class, machineDrain);
    ExecutorMessageMux.demux(message, callbacks);
  }

  @Test
  public void testAllTypesMapped() {
    control.replay();

    for (ExecutorMessageType type : ExecutorMessageType.values()) {
      assertNotNull(ExecutorMessageMux.TYPE_MAPPING.get(type));
    }
  }
}
