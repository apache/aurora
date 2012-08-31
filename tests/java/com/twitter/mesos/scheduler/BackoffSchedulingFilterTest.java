package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.SchedulingFilter.Veto;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

import static com.twitter.mesos.scheduler.BackoffSchedulingFilter.BACKOFF_VETO;

public class BackoffSchedulingFilterTest extends EasyMockTest {

  private static final TwitterTaskInfo TASK = new TwitterTaskInfo();
  private static final String HOST = "host";
  private static final String TASK_ID = "task_id";

  private static final Resources RESOURCES = Resources.from(new TwitterTaskInfo());

  private ScheduleBackoff backoff;
  private SchedulingFilter delegate;

  private SchedulingFilter filter;

  @Before
  public void setUp() {
    backoff = createMock(ScheduleBackoff.class);
    delegate = createMock(SchedulingFilter.class);
    filter = new BackoffSchedulingFilter(backoff, delegate);
  }

  @Test
  public void testBackoff() {
    expect(backoff.isSchedulable(TASK)).andReturn(false);
    control.replay();
    assertEquals(BACKOFF_VETO, filter.filter(RESOURCES, HOST, TASK, TASK_ID));
  }

  @Test
  public void testSchedules() {
    expect(backoff.isSchedulable(TASK)).andReturn(true);
    expect(delegate.filter(RESOURCES, HOST, TASK, TASK_ID)).andReturn(ImmutableSet.<Veto>of());
    control.replay();
    assertEquals(ImmutableSet.<Veto>of(), filter.filter(RESOURCES, HOST, TASK, TASK_ID));
  }

  @Test
  public void testDelegateVetoes() {
    Set<Veto> vetoes = ImmutableSet.of(new Veto("Fake", 1));

    expect(backoff.isSchedulable(TASK)).andReturn(true);
    expect(delegate.filter(RESOURCES, HOST, TASK, TASK_ID)).andReturn(vetoes);
    control.replay();
    assertEquals(vetoes, filter.filter(RESOURCES, HOST, TASK, TASK_ID));
  }
}
