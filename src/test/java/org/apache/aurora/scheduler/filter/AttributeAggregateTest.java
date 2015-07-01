/**
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

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multiset;
import com.twitter.common.collections.Pair;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Attribute;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class AttributeAggregateTest extends EasyMockTest {
  private AttributeStore attributeStore;

  @Before
  public void setUp() throws Exception {
    attributeStore = createMock(AttributeStore.class);
  }

  @Test
  public void testNoTasks() {
    control.replay();

    AttributeAggregate aggregate = aggregate();
    assertEquals(ImmutableMultiset.<Pair<String, String>>of(), aggregate.getAggregates());
    assertAggregate(aggregate, "none", "alsoNone", 0);
  }

  @Test(expected = IllegalStateException.class)
  public void testAttributesMissing() {
    expect(attributeStore.getHostAttributes("a")).andReturn(Optional.absent());

    control.replay();

    aggregate(task("1", "a")).getAggregates();
  }

  @Test(expected = NullPointerException.class)
  public void testTaskWithNoHost() {
    control.replay();

    aggregate(task("1", null)).getAggregates();
  }

  @Test
  public void testNoAttributes() {
    expectGetAttributes("hostA");

    control.replay();

    assertEquals(
        ImmutableMultiset.<Pair<String, String>>of(),
        aggregate(task("1", "hostA")).getAggregates());
  }

  @Test
  public void testAggregate() {
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

    Multiset<Pair<String, String>> expected = ImmutableMultiset.<Pair<String, String>>builder()
        .add(Pair.of("rack", "a"))
        .addCopies(Pair.of("rack", "b"), 3)
        .add(Pair.of("rack", "c"))
        .add(Pair.of("host", "a1"))
        .addCopies(Pair.of("host", "b1"), 2)
        .add(Pair.of("host", "b2"))
        .add(Pair.of("host", "c1"))
        .addCopies(Pair.of("pdu", "p1"), 4)
        .addCopies(Pair.of("pdu", "p2"), 4)
        .add(Pair.of("ssd", "true"))
        .build();
    AttributeAggregate aggregate = aggregate(
        task("1", "a1"),
        task("2", "b1"),
        task("3", "b1"),
        task("4", "b2"),
        task("5", "c1"));
    assertEquals(expected, aggregate.getAggregates());
    for (Multiset.Entry<Pair<String, String>> entry : expected.entrySet()) {
      Pair<String, String> element = entry.getElement();
      assertAggregate(aggregate, element.getFirst(), element.getSecond(), entry.getCount());
    }
    assertAggregate(aggregate, "host", "c2", 0L);
    assertAggregate(aggregate, "hostc", "2", 0L);
  }

  private AttributeAggregate aggregate(IScheduledTask... activeTasks) {
    return AttributeAggregate.create(
        Suppliers.ofInstance(ImmutableSet.copyOf(activeTasks)),
        attributeStore);
  }

  private IExpectationSetters<?> expectGetAttributes(String host, Attribute... attributes) {
    return expect(attributeStore.getHostAttributes(host)).andReturn(Optional.of(
        IHostAttributes.build(new HostAttributes()
            .setHost(host)
            .setAttributes(ImmutableSet.copyOf(attributes)))));
  }

  private void assertAggregate(
      AttributeAggregate aggregate,
      String name,
      String value,
      long expected) {

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
        .setValues(ImmutableSet.copyOf(values));
  }
}
