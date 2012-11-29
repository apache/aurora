package com.twitter.mesos.scheduler;

import java.util.concurrent.atomic.AtomicLong;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.stats.StatsProvider;
import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.scheduler.events.PubsubEvent.TaskStateChange;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.MutableStoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.MutateWork;
import com.twitter.mesos.scheduler.storage.mem.MemStorage;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LostTaskStatsTest extends EasyMockTest {

  private Storage storage;
  private StatsProvider statsProvider;
  private LostTaskStats lostStats;

  @Before
  public void setUp() {
    storage = MemStorage.newEmptyStorage();
    statsProvider = createMock(StatsProvider.class);
    lostStats = new LostTaskStats(storage, statsProvider);
  }

  @Test
  public void testCounters() {
    AtomicLong rackA = new AtomicLong();
    expect(statsProvider.makeCounter(LostTaskStats.statName("rackA"))).andReturn(rackA);
    AtomicLong rackB = new AtomicLong();
    expect(statsProvider.makeCounter(LostTaskStats.statName("rackB"))).andReturn(rackB);

    control.replay();

    setRack("host1", "rackA");
    setRack("host2", "rackB");
    setRack("host3", "rackB");
    taskLost("host1");
    taskLost("host2");
    taskLost("host3");
    taskLost("host1");

    assertEquals(2, rackA.get());
    assertEquals(2, rackB.get());
  }

  @Test
  public void testRackMissing() {
    control.replay();
    taskLost("a");
  }

  private void taskLost(String host) {
    lostStats.stateChange(new TaskStateChange(
        "task-" + host,
        ScheduleStatus.RUNNING,
        new ScheduledTask()
            .setStatus(ScheduleStatus.LOST)
            .setAssignedTask(new AssignedTask().setSlaveHost(host)))
    );
  }

  private void setRack(final String host, final String rack) {
    storage.doInWriteTransaction(new MutateWork.NoResult.Quiet() {
      @Override protected void execute(MutableStoreProvider storeProvider) {
        storeProvider.getAttributeStore().saveHostAttributes(
            new HostAttributes()
                .setHost(host)
                .setAttributes(ImmutableSet.of(
                    new Attribute().setName("rack").setValues(ImmutableSet.of(rack)))));
      }
    });
  }
}
