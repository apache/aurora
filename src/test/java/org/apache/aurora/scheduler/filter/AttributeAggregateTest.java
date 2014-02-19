/**
 * Copyright 2014 Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.filter;

import java.util.Map;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class AttributeAggregateTest extends EasyMockTest {

  private Supplier<ImmutableSet<IScheduledTask>> activeTaskSupplier;
  private AttributeStore attributeStore;
  private AttributeAggregate aggregate;

  @Before
  public void setUp() throws Exception {
    activeTaskSupplier = createMock(new Clazz<Supplier<ImmutableSet<IScheduledTask>>>() { });
    attributeStore = createMock(AttributeStore.class);
    aggregate = new AttributeAggregate(activeTaskSupplier, attributeStore);
  }

  @Test
  public void testNoTasks() {
    expectGetTasks();

    control.replay();

    assertAggregates(ImmutableMap.<Pair<String, String>, Long>of());
    assertAggregate("none", "alsoNone", 0);
  }

  @Test(expected = IllegalStateException.class)
  public void testAttributesMissing() {
    expectGetTasks(task("1", "a"));
    expect(attributeStore.getHostAttributes("a")).andReturn(Optional.<HostAttributes>absent());

    control.replay();

    aggregate.getAggregates();
  }

  @Test(expected = NullPointerException.class)
  public void testTaskWithNoHost() {
    expectGetTasks(task("1", null));

    control.replay();

    aggregate.getAggregates();
  }

  @Test
  public void testNoAttributes() {
    expectGetTasks(task("1", "hostA"));
    expectGetAttributes("hostA");

    control.replay();

    assertAggregates(ImmutableMap.<Pair<String, String>, Long>of());
  }

  @Test
  public void testAggregate() {
    expectGetTasks(
        task("1", "a1"),
        task("2", "b1"),
        task("3", "b1"),
        task("4", "b2"),
        task("5", "c1"));
    expectGetAttributes(
        "a1",
        attribute("host", "a1"),
        attribute("rack", "a"),
        attribute("pdu", "p1"));
    expectGetAttributes(
        "b1",
        attribute("host", "b1"),
        attribute("rack", "b"),
        attribute("pdu", "p1", "p2"))
        .times(2);
    expectGetAttributes(
        "b2",
        attribute("host", "b2"),
        attribute("rack", "b"),
        attribute("pdu", "p1", "p2"));
    expectGetAttributes(
        "c1",
        attribute("host", "c1"),
        attribute("rack", "c"),
        attribute("pdu", "p2"),
        attribute("ssd", "true"));

    control.replay();

    Map<Pair<String, String>, Long> expected = ImmutableMap.<Pair<String, String>, Long>builder()
        .put(Pair.of("rack", "a"), 1L)
        .put(Pair.of("rack", "b"), 3L)
        .put(Pair.of("rack", "c"), 1L)
        .put(Pair.of("host", "a1"), 1L)
        .put(Pair.of("host", "b1"), 2L)
        .put(Pair.of("host", "b2"), 1L)
        .put(Pair.of("host", "c1"), 1L)
        .put(Pair.of("pdu", "p1"), 4L)
        .put(Pair.of("pdu", "p2"), 4L)
        .put(Pair.of("ssd", "true"), 1L)
        .build();
    assertAggregates(expected);
    for (Map.Entry<Pair<String, String>, Long> entry : expected.entrySet()) {
      assertAggregate(entry.getKey().getFirst(), entry.getKey().getSecond(), entry.getValue());
    }
    assertAggregate("host", "c2", 0L);
    assertAggregate("hostc", "2", 0L);
  }

  private void expectGetTasks(IScheduledTask... activeTasks) {
    expect(activeTaskSupplier.get())
        .andReturn(ImmutableSet.<IScheduledTask>builder().add(activeTasks).build());
  }

  private IExpectationSetters<?> expectGetAttributes(String host, Attribute... attributes) {
    return expect(attributeStore.getHostAttributes(host)).andReturn(Optional.of(
        new HostAttributes()
            .setHost(host)
            .setAttributes(ImmutableSet.<Attribute>builder().add(attributes).build())));
  }

  private void assertAggregates(Map<Pair<String, String>, Long> expected) {
    assertEquals(expected, aggregate.getAggregates());
  }

  private void assertAggregate(String name, String value, long expected) {
    assertEquals(expected, aggregate.getNumTasksWithAttribute(name, value));
  }

  private static IScheduledTask task(String id, String host) {
    return IScheduledTask.build(new ScheduledTask().setAssignedTask(
        new AssignedTask()
            .setTaskId(id)
            .setSlaveHost(host)));
  }

  private Attribute attribute(String name, String... values) {
    return new Attribute()
        .setName(name)
        .setValues(ImmutableSet.<String>builder().add(values).build());
  }
}
